package ctlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"

	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/globalstats"
	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/scanfunc"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlgen"
)

// LDBReader reads data from the LDB. The external interface is
// thread-safe and it is safe to create as many of these as needed
// across multiple processes.
type LDBReader struct {
	Db                          *sql.DB
	pkCache                     map[string]schema.PrimaryKey // keyed by ldbTableName()
	getRowByKeyStmtCache        map[string]*sql.Stmt         // keyed by ldbTableName()
	getRowsByKeyPrefixStmtCache map[prefixCacheKey]*sql.Stmt
	mu                          sync.RWMutex
	cancelWatcher               context.CancelFunc
}

type prefixCacheKey struct {
	ldbTableName string
	numKeys      int
}

var (
	ErrTableHasNoPrimaryKey = errors.New("Table provided has no primary key")
	ErrNeedFullKey          = errors.New("All primary key fields are required")
	ErrNoLedgerUpdates      = errors.New("no ledger updates have been received yet")
)

func newLDBReader(path string) (*LDBReader, error) {
	db, err := newLDB(path)
	if err != nil {
		return nil, err
	}

	return &LDBReader{Db: db}, nil
}

func newVersionedLDBReader(dirPath string) (*LDBReader, error) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &LDBReader{
		cancelWatcher: cancel,
	}

	// To initialize this reader, we must first load an LDB:
	last, err := lookupLastLDBSync(dirPath)
	if err != nil {
		return nil, fmt.Errorf("checking last ldb sync: %w", err)
	}
	if last == 0 {
		return nil, fmt.Errorf("no LDB in path (%s)", dirPath)
	}
	err = reader.switchLDB(dirPath, last)
	if err != nil {
		return nil, fmt.Errorf("switching ldbs: %w", err)
	}

	// Then we can defer to the watcher goroutine to swap this
	// reader LDB if a newer one appears:
	go reader.watchForLDBs(ctx, dirPath, last)

	return reader, nil
}

func newLDB(path string) (*sql.DB, error) {
	_, err := os.Stat(path)
	switch {
	case os.IsNotExist(err):
		return nil, fmt.Errorf("no LDB found at %s", path)
	case err != nil:
		return nil, err
	}

	var db *sql.DB
	if ldbVersioning {
		db, err = ldb.OpenImmutableLDB(path)
	} else {
		mode := "ro"
		if !globalLDBReadOnly {
			mode = "rwc"
		}

		db, err = ldb.OpenLDB(path, mode)
	}
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Constructs an LDBReader from a sql.DB. Really only useful for testing.
func NewLDBReaderFromDB(db *sql.DB) *LDBReader {
	return &LDBReader{Db: db}
}

// GetLastSequence returns the highest sequence number applied to the DB
func (reader *LDBReader) GetLastSequence(ctx context.Context) (schema.DMLSequence, error) {
	ctx = discardContext()
	reader.mu.RLock()
	defer reader.mu.RUnlock()
	return ldb.FetchSeqFromLdb(ctx, reader.Db)
}

// GetLedgerLatency returns the difference between the current time and the timestamp
// from the last DML ledger update processed by the reflector. ErrNoLedgerUpdates will
// be returned if no DML statements have been processed.
func (reader *LDBReader) GetLedgerLatency(ctx context.Context) (time.Duration, error) {
	ctx = discardContext()
	row := reader.Db.QueryRowContext(ctx, "select timestamp from "+ldb.LDBLastUpdateTableName+" where name=?", ldb.LDBLastLedgerUpdateColumn)
	var timestamp time.Time
	err := row.Scan(&timestamp)
	switch {
	case err == sql.ErrNoRows:
		return 0, ErrNoLedgerUpdates
	case err != nil:
		return 0, fmt.Errorf("get ledger latency: %w", err)
	default:
		return time.Now().Sub(timestamp), nil
	}
}

// GetRowsByKeyPrefix returns a *Rows iterator that will supply all of the rows in
// the family and table match the supplied primary key prefix.
func (reader *LDBReader) GetRowsByKeyPrefix(ctx context.Context, familyName string, tableName string, key ...interface{}) (*Rows, error) {
	ctx = discardContext()
	start := time.Now()
	defer func() {
		globalstats.Observe("get_rows_by_key_prefix", time.Now().Sub(start),
			stats.T("family", familyName),
			stats.T("table", tableName))
	}()

	reader.mu.RLock()
	defer reader.mu.RUnlock()
	famName, err := schema.NewFamilyName(familyName)
	if err != nil {
		return nil, err
	}
	tblName, err := schema.NewTableName(tableName)
	if err != nil {
		return nil, err
	}
	ldbTable := schema.LDBTableName(famName, tblName)
	pk, err := reader.getPrimaryKey(ctx, ldbTable)
	if err != nil {
		return nil, err
	}
	if pk.Zero() {
		return nil, ErrTableHasNoPrimaryKey
	}
	if len(key) > len(pk.Fields) {
		return nil, errors.New("too many keys supplied for table's primary key")
	}
	err = convertKeyBeforeQuery(pk, key)
	if err != nil {
		return nil, err
	}
	stmt, err := reader.getRowsByKeyPrefixStmt(ctx, pk, ldbTable, len(key))
	if err != nil {
		return nil, err
	}
	if len(key) == 0 {
		globalstats.Incr("full-table-scans", familyName, tableName)
	}
	rows, err := stmt.QueryContext(ctx, key...)
	switch {
	case err == nil:
		cols, err := schema.DBColumnMetaFromRows(rows)
		if err != nil {
			return nil, err
		}
		res := &Rows{rows: rows, cols: cols}
		return res, nil
	case err == sql.ErrNoRows:
		return &Rows{}, nil
	default:
		return nil, err
	}
}

// GetRowByKey fetches a row from the supplied table by the key parameter,
// filling the data into the out param.
//
// The out param may be one of the following types:
//   - pointer to struct
//   - map[string]interface{}
//
// The key parameter can support composite keys by passing a slice type.
func (reader *LDBReader) GetRowByKey(
	ctx context.Context,
	out interface{},
	familyName string,
	tableName string,
	key ...interface{},
) (found bool, err error) {
	ctx = discardContext()
	start := time.Now()
	defer func() {
		globalstats.Observe("get_row_by_key", time.Now().Sub(start),
			stats.T("family", familyName),
			stats.T("table", tableName))
	}()

	reader.mu.RLock()
	defer reader.mu.RUnlock()

	famName, err := schema.NewFamilyName(familyName)
	if err != nil {
		return
	}

	tblName, err := schema.NewTableName(tableName)
	if err != nil {
		return
	}

	ldbTable := schema.LDBTableName(famName, tblName)

	// NOTE: A persistent cache is kept on the reader to avoid needing
	// to query for PKs on every call. Given that most API consumers will
	// very likely use the global singleton reader, this means that we
	// must assume that the cache will be shared across the whole process.
	// The way that a PK would be changed on a table is that it would need
	// to be dropped and re-created. In the mean time, this cache will
	// go stale. The way that this is dealt with is to clear the cache if
	// the statement encounters any execution errors.
	pk, err := reader.getPrimaryKey(ctx, ldbTable) // assumes RLock held
	if err != nil {
		return
	}

	if pk.Zero() {
		err = ErrTableHasNoPrimaryKey
		return
	}

	if len(pk.Fields) != len(key) {
		err = ErrNeedFullKey
		return
	}

	// Stmt & PK cache are separate now to give the option to gracefully
	// move back.
	stmt, err := reader.getGetRowByKeyStmt(ctx, pk, ldbTable) // assumes RLock held
	if err != nil {
		return
	}

	err = convertKeyBeforeQuery(pk, key)
	if err != nil {
		return
	}

	rows, err := stmt.QueryContext(ctx, key...)
	if err == sql.ErrNoRows {
		found = false
		err = nil
		rows.Close()
		return
	}
	if err != nil {
		// See NOTE above about why this cache is getting cleared
		reader.invalidatePKCache(ldbTable) // assumes RLock is held
		err = fmt.Errorf("query target row error: %w", err)
		return
	}
	defer rows.Close()

	cols, err := schema.DBColumnMetaFromRows(rows)
	if err != nil {
		return
	}

	scanFunc, err := scanfunc.New(out, cols)
	if err != nil {
		return
	}

	if !rows.Next() {
		// found is already false by default
		err = rows.Err()
		return
	}

	found = true
	err = scanFunc(rows)

	if err != nil {
		err = fmt.Errorf("target row scan error: %w", err)
	} else {
		err = rows.Err()
	}

	return
}

func (reader *LDBReader) Close() error {
	reader.mu.Lock()
	defer reader.mu.Unlock()

	if reader.cancelWatcher != nil {
		reader.cancelWatcher()
	}

	return reader.closeDB()
}

// closeDB closes all reader-owned resources associated with the current DB.
// It should only be called when the caller is holding the reader.mu mutex.
func (reader *LDBReader) closeDB() error {
	for _, stmt := range reader.getRowByKeyStmtCache {
		if err := stmt.Close(); err != nil {
			return err
		}
	}
	reader.getRowByKeyStmtCache = map[string]*sql.Stmt{}
	for _, stmt := range reader.getRowsByKeyPrefixStmtCache {
		if err := stmt.Close(); err != nil {
			return err
		}
	}
	reader.getRowsByKeyPrefixStmtCache = map[prefixCacheKey]*sql.Stmt{}

	if reader.Db != nil {
		return reader.Db.Close()
	}

	return nil
}

// Ping checks if the LDB is available
func (reader *LDBReader) Ping(ctx context.Context) bool {
	ctx = discardContext()
	reader.mu.RLock()
	defer reader.mu.RUnlock()

	qs := "SELECT seq FROM " + ldb.LDBSeqTableName + " WHERE id = ?"
	row := reader.Db.QueryRowContext(ctx, qs, ldb.LDBSeqTableID)

	var seq sql.NullInt64
	err := row.Scan(&seq)
	if err != nil || !seq.Valid {
		return false
	}
	return true
}

// ensure that a supplied key is converted appropriately with respect
// to the type of each PK column.
func convertKeyBeforeQuery(pk schema.PrimaryKey, key []interface{}) error {
	for i, k := range key {
		// sanity check on th elength of the pk field type slice
		if i >= len(pk.Types) {
			return errors.New("insufficient key field type data")
		}
		pkt := pk.Types[i]
		switch k := k.(type) {
		case string:
			switch pkt {
			case schema.FTBinary, schema.FTByteString:
				// convert the key from a string -> []byte so that the
				// types match, otherwise it won't find the row.
				key[i] = []byte(k)
			}
		}
	}
	return nil
}

func (reader *LDBReader) lock() {
	reader.mu.Lock()
}

func (reader *LDBReader) unlock() {
	reader.mu.Unlock()
}

// WARNING: assumes mutex is read locked
func (reader *LDBReader) invalidatePKCache(ldbTable string) {
	if reader.pkCache == nil {
		// Cache hasn't even been initialized yet, so invalidation would
		// do nothing anyways.
		return
	}

	reader.mu.RUnlock()
	reader.mu.Lock()
	delete(reader.pkCache, ldbTable)
	reader.mu.Unlock()
	reader.mu.RLock()
}

// WARNING: assumes mutex is read locked
func (reader *LDBReader) getPrimaryKey(ctx context.Context, ldbTable string) (schema.PrimaryKey, error) {
	if reader.pkCache == nil {
		reader.mu.RUnlock()
		reader.mu.Lock()

		// double check because there could be a race which would result
		// in us wiping out the cache
		if reader.pkCache == nil {
			reader.pkCache = make(map[string]schema.PrimaryKey)
		}

		reader.mu.Unlock()
		reader.mu.RLock()
	}

	if _, found := reader.pkCache[ldbTable]; !found {
		const qs = "SELECT name,type FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk ASC"
		rows, err := reader.Db.QueryContext(ctx, qs, ldbTable)
		if err != nil {
			return schema.PrimaryKeyZero, fmt.Errorf("query pragma_table_info error: %w", err)
		}
		defer rows.Close()

		rawFieldNames := []string{}
		rawFieldTypes := []string{}
		for rows.Next() {
			var name string
			var ftString string
			err = rows.Scan(&name, &ftString)
			if err != nil {
				return schema.PrimaryKeyZero, fmt.Errorf("scan: %w", err)
			}
			rawFieldNames = append(rawFieldNames, name)
			rawFieldTypes = append(rawFieldTypes, ftString)
		}
		err = rows.Err()
		if err != nil {
			return schema.PrimaryKeyZero, fmt.Errorf("rows err: %w", err)
		}

		pk, err := schema.NewPKFromRawNamesAndTypes(rawFieldNames, rawFieldTypes)
		if err != nil {
			return schema.PrimaryKeyZero, err
		}

		if pk.Zero() {
			// There's a potential that this is a missing table, so check
			// that as well.
			qs := sqlgen.SqlSprintf("SELECT * FROM $1 LIMIT 1", ldbTable)
			_, err := reader.Db.ExecContext(ctx, qs)
			if err != nil {
				if strings.Index(err.Error(), "no such table:") == 0 {
					return schema.PrimaryKeyZero, errors.New("Table not found")
				}
				return schema.PrimaryKeyZero, err
			}
		}

		// Hold the lock for a tiny amount of time. That means there is
		// a chance for races to cause multiple executions of this block
		// of code that wastefully do the same thing. That's worth it
		// to avoid per-key caching complexity and to keep the lock holding
		// time very short.
		reader.mu.RUnlock()
		reader.mu.Lock()
		reader.pkCache[ldbTable] = pk
		reader.mu.Unlock()
		reader.mu.RLock()

		return pk, nil
	}

	return reader.pkCache[ldbTable], nil
}

func (reader *LDBReader) getRowsByKeyPrefixStmt(ctx context.Context, pk schema.PrimaryKey, ldbTable string, numKeys int) (*sql.Stmt, error) {
	// assumes RLock is held
	if reader.getRowsByKeyPrefixStmtCache == nil {
		reader.mu.RUnlock()
		reader.mu.Lock()
		// double check because there could be a race which would result
		// in us wiping out the cache
		if reader.getRowsByKeyPrefixStmtCache == nil {
			reader.getRowsByKeyPrefixStmtCache = make(map[prefixCacheKey]*sql.Stmt)
		}
		reader.mu.Unlock()
		reader.mu.RLock()
	}
	pck := prefixCacheKey{ldbTableName: ldbTable, numKeys: numKeys}
	stmt, found := reader.getRowsByKeyPrefixStmtCache[pck]
	if found {
		return stmt, nil
	}

	reader.mu.RUnlock()
	defer reader.mu.RLock()
	reader.mu.Lock()
	defer reader.mu.Unlock()

	qsTokens := []string{
		"SELECT * FROM",
		ldbTable,
	}
	if numKeys > 0 {
		qsTokens = append(qsTokens, "WHERE")
		for i := 0; i < numKeys; i++ {
			pkField := pk.Fields[i]
			if i > 0 {
				qsTokens = append(qsTokens, "AND")
			}
			qsTokens = append(qsTokens,
				pkField.Name,
				"=",
				"?")
		}
	}
	qs := strings.Join(qsTokens, " ")
	stmt, err := reader.Db.PrepareContext(ctx, qs)
	if err == nil {
		reader.getRowsByKeyPrefixStmtCache[pck] = stmt
	}
	return stmt, err
}

func (reader *LDBReader) getGetRowByKeyStmt(ctx context.Context, pk schema.PrimaryKey, ldbTable string) (*sql.Stmt, error) {
	// assumes RLock is held
	if reader.getRowByKeyStmtCache == nil {
		reader.mu.RUnlock()
		reader.mu.Lock()

		// double check because there could be a race which would result
		// in us wiping out the cache
		if reader.getRowByKeyStmtCache == nil {
			reader.getRowByKeyStmtCache = make(map[string]*sql.Stmt)
		}

		reader.mu.Unlock()
		reader.mu.RLock()
	}

	stmt, found := reader.getRowByKeyStmtCache[ldbTable]
	if found {
		return stmt, nil
	}

	reader.mu.RUnlock()
	defer reader.mu.RLock()
	reader.mu.Lock()
	defer reader.mu.Unlock()

	qsTokens := []string{
		"SELECT * FROM",
		ldbTable,
		"WHERE",
	}

	for i, pkField := range pk.Fields {
		if i > 0 {
			qsTokens = append(qsTokens, "AND")
		}
		qsTokens = append(qsTokens,
			pkField.Name,
			"=",
			"?")
	}

	qs := strings.Join(qsTokens, " ")
	stmt, err := reader.Db.PrepareContext(ctx, qs)
	if err == nil {
		reader.getRowByKeyStmtCache[ldbTable] = stmt
	}

	return stmt, err
}

func (reader *LDBReader) watchForLDBs(ctx context.Context, dirPath string, last int64) {
	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fsLast, err := lookupLastLDBSync(dirPath)
			if err != nil {
				events.Log("failed checking for last LDB sync: %{error}+v", err)
				errs.Incr("check-last-ldb-sync")
				continue
			}

			// Only swap LDBs if this LDB is newer than our newest LDB.
			if fsLast <= last {
				continue
			}
			events.Log("found new LDB (%d > %d), switching...", fsLast, last)
			last = fsLast

			err = reader.switchLDB(dirPath, last)
			if err != nil {
				events.Log("failed switching to new LDB: %{error}+v", err)
				errs.Incr("switch-ldb")
			}
		}
	}
}

func (reader *LDBReader) switchLDB(dirPath string, timestamp int64) error {
	fullPath := filepath.Join(dirPath, fmt.Sprintf("%013d", timestamp), ldb.DefaultLDBFilename)

	db, err := newLDB(fullPath)
	if err != nil {
		return fmt.Errorf("new ldb: %w", err)
	}

	reader.mu.Lock()
	defer reader.mu.Unlock()

	if err = reader.closeDB(); err != nil {
		return fmt.Errorf("closing db: %w", err)
	}

	reader.Db = db

	return nil
}

func lookupLastLDBSync(dirPath string) (int64, error) {
	// Loop through the files in the `dirPath` and look for the ldb.db with the
	// highest associated timestamp. Return that.
	var lastSync int64
	err := filepath.Walk(dirPath, func(filePath string, info os.FileInfo, err error) error {
		// Bail if we hit any errors while visiting files/directories:
		if info == nil || err != nil {
			return err
		}

		// Ignore directories:
		if info.IsDir() {
			return nil
		}

		// We only care about the timestamps associated with `<path>/<timestamp>/ldb.db` files.
		// Therefore, we ignore all other files (ldb.db.wal, etc).
		if !strings.HasSuffix(filePath, ldb.DefaultLDBFilename) {
			return nil
		}

		// Ignore `<path>/ldb.db`, which is the standard path for LDBs. We only care
		// about versioned LDBs, which are those in a timestamped directory.
		if strings.HasPrefix(filePath, filepath.Join(dirPath, ldb.DefaultLDBFilename)) {
			return nil
		}

		// Omit the root path from the file path:
		// dirPath + ["<timestamp>", ldb.DefaultLDBFilename]
		localPath, err := filepath.Rel(dirPath, filePath)
		if err != nil {
			return fmt.Errorf("base path (%s): %w", filePath, err)
		}
		fields := strings.Split(localPath, "/")

		if len(fields) != 2 || fields[1] != ldb.DefaultLDBFilename {
			events.Log("ignoring unexpected file in LDB path (%+v)", fields)
			errs.Incr("unexpected-local-file")
			return nil
		}
		timestamp, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			events.Log("ignoring file with invalid timestamp in LDB path (%+v)", fields)
			errs.Incr("invalid-timestamp-local-file")
			return nil
		}

		if timestamp > lastSync {
			lastSync = timestamp
		}

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("filepath walk: %w", err)
	}

	return lastSync, nil
}

// discardContext returns context.Background(). the exported reader API uses the returned
// value instead. This is done because the underlying sqlite CGO code that
// the reader API ultimately calls does not handle interruptions optimally. Additionally
// because the calls read from disk instead of making network calls, context cancellation is
// arguably less important to begin with.
func discardContext() context.Context {
	return context.Background()
}
