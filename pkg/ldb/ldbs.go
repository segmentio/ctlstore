package ldb

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/segmentio/ctlstore/pkg/schema"
)

const (
	LDBSeqTableName           = "_ldb_seq"
	LDBLastUpdateTableName    = "_ldb_last_update"
	LDBLastLedgerUpdateColumn = "ledger"
	LDBSeqTableID             = 1
	LDBDatabaseDriver         = "sqlite3"
	DefaultLDBFilename        = "ldb.db"
)

var (
	// SQL for fetching current tracked sequence
	ldbFetchSeqSQL = fmt.Sprintf(`
		SELECT seq FROM %s WHERE id = %d
		`, LDBSeqTableName, LDBSeqTableID)

	ldbInitializeDDLs = []string{
		// Initialization DDL for table that tracks sequence position. Tried to avoid
		// a PK column but it makes updating the sequence monotonically messy.
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY NOT NULL,
			seq BIGINT NOT NULL
		)`, LDBSeqTableName),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			name STRING PRIMARY KEY NOT NULL,
			timestamp DATETIME NOT NULL
		)`, LDBLastUpdateTableName),
	}
)

var testTmpSeq int64 = 0

func LDBForTest(t testing.TB) (res *sql.DB, teardown func()) {
	tmpDir, err := ioutil.TempDir("", "ldb-for-test")
	if err != nil {
		t.Fatal(err)
	}

	// Since there's a need for multiple TXs, have to use a tmp file
	// for the database. In-memory in shared-cache mode kinda works,
	// but it has aggressive locking that blocks the tests we want to do.
	db, err := OpenLDB(NextTestLdbTmpPath(tmpDir), "rwc")
	if err != nil {
		t.Fatalf("Couldn't open SQLite db, error %v", err)
	}
	err = EnsureLdbInitialized(context.Background(), db)
	if err != nil {
		t.Fatalf("Couldn't initialize SQLite db, error %v", err)
	}
	return db, func() {
		if tmpDir != "" {
			os.RemoveAll(tmpDir)
		}
	}
}

func OpenLDB(path string, mode string) (*sql.DB, error) {
	return sql.Open("sqlite3_with_autocheckpoint_off",
		fmt.Sprintf("file:%s?_journal_mode=wal&mode=%s", path, mode))
}

func OpenImmutableLDB(path string) (*sql.DB, error) {
	return sql.Open("sqlite3_with_autocheckpoint_off", fmt.Sprintf("file:%s?immutable=true", path))
}

// Ensures the LDB is prepared for queries
func EnsureLdbInitialized(ctx context.Context, db *sql.DB) error {
	for _, statement := range ldbInitializeDDLs {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func NewLDBTmpPath(t *testing.T) (string, func()) {
	path, err := ioutil.TempDir("", "ldb-tmp-path")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(path, "ldb.db")
	return dbPath, func() {
		if path != "" {
			os.RemoveAll(path)
		}
	}
}

func NextTestLdbTmpPath(testTmpDir string) string {
	nextSeq := atomic.AddInt64(&testTmpSeq, 1)
	return fmt.Sprintf("%s/ldbForTest%d.db", testTmpDir, nextSeq)
}

// Gets current sequence from provided db
func FetchSeqFromLdb(ctx context.Context, db *sql.DB) (schema.DMLSequence, error) {
	row := db.QueryRowContext(ctx, ldbFetchSeqSQL)
	var seq int64
	err := row.Scan(&seq)
	if err == sql.ErrNoRows {
		return schema.DMLSequence(0), nil
	}
	return schema.DMLSequence(seq), err
}
