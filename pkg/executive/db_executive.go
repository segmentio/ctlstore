package executive

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/scanfunc"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlgen"
	"github.com/segmentio/events/v2"
)

const dmlLedgerTableName = "ctlstore_dml_ledger"
const mutatorsTableName = "mutators"

// A database-backed (ctldb) Executive.
type dbExecutive struct {
	DB      *sql.DB
	limiter *dbLimiter
	Ctx     context.Context
}

// TODO: check CancelFuncs everywhere for leakin

// Called to "fork" the context from the original, for internal use
func (e *dbExecutive) ctx() (context.Context, context.CancelFunc) {
	if e.Ctx == nil {
		// No parent context specified, so any context will do.
		e.Ctx = context.Background()
	}

	return context.WithCancel(e.Ctx)
}

func (e *dbExecutive) CreateFamily(familyName string) error {
	ctx, cancel := e.ctx()
	defer cancel()

	famName, err := schema.NewFamilyName(familyName)
	if err != nil {
		return err
	}

	qs := "INSERT INTO families (name) VALUES(?)"

	_, err = e.DB.ExecContext(ctx, qs, famName.Name)
	if err != nil {
		if errorIsRowConflict(err) {
			return &errs.ConflictError{Err: "Family already exists"}
		}
		return err
	}

	return nil
}

func (e *dbExecutive) CreateTable(familyName string, tableName string, fieldNames []string, fieldTypes []schema.FieldType, keyFields []string) error {
	ctx, cancel := e.ctx()
	defer cancel()

	famName, _, tbl, err := sqlgen.BuildMetaTableFromInput(
		sqlgen.SqlDriverToDriverName(e.DB.Driver()),
		familyName,
		tableName,
		fieldNames,
		fieldTypes,
		keyFields,
	)
	if err != nil {
		return err
	}

	if len(tbl.KeyFields.Fields) == 0 {
		return &errs.BadRequestError{Err: "table must have at least one key field"}
	}

	_, ok, err := e.fetchFamilyByName(famName)
	if err != nil {
		return err
	}
	if !ok {
		return &errs.NotFoundError{Err: "Family not found"}
	}

	err = tbl.Validate()
	if err != nil {
		return &errs.BadRequestError{err.Error()}
	}

	ddl, err := tbl.AsCreateTableDDL()
	if err != nil {
		return err
	}

	dmlLogTbl, err := tbl.ForDriver(ldb.LDBDatabaseDriver)
	if err != nil {
		return err
	}

	logDDL, err := dmlLogTbl.AsCreateTableDDL()
	if err != nil {
		return err
	}

	events.Debug("[CreateTable %{tableName}s] ctldb DDL: %{ddl}s", tableName, ddl)
	events.Debug("[CreateTable %{tableName}s] log DDL: %{ddl}s", tableName, logDDL)

	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	dlw := dmlLedgerWriter{
		Tx:        tx,
		TableName: dmlLedgerTableName,
	}
	defer dlw.Close()

	_, err = tx.ExecContext(ctx, ddl)
	if err != nil {
		if strings.Index(err.Error(), "Error 1050:") == 0 ||
			strings.Contains(err.Error(), "already exists") {
			return &errs.ConflictError{Err: "Table already exists"}
		}
		return err
	}

	seq, err := dlw.Add(ctx, logDDL)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	events.Log("Successfully created new table `%{tableName}s` at seq %{seq}v", tableName, seq)

	return nil
}

func (e *dbExecutive) AddFields(familyName string, tableName string, fieldNames []string, fieldTypes []schema.FieldType) error {
	ctx, cancel := e.ctx()
	defer cancel()
	// We create a metatable here with no fields. We will
	// ask it to create DDL to add each field.
	famName, _, tbl, err := sqlgen.BuildMetaTableFromInput(
		sqlgen.SqlDriverToDriverName(e.DB.Driver()),
		familyName,
		tableName,
		nil,
		nil,
		nil,
	)
	_, ok, err := e.fetchFamilyByName(famName)
	if err != nil {
		return err
	}
	if !ok {
		return &errs.NotFoundError{Err: "Family does not exist"}
	}
	if lfn, lft := len(fieldNames), len(fieldTypes); lfn != lft {
		return &errs.BadRequestError{Err: fmt.Sprintf("number of fields (%d) != number of types (%d)", lfn, lft)}
	}
	for i, fieldName := range fieldNames {
		fn, err := schema.NewFieldName(fieldName)
		if err != nil {
			return err
		}
		fieldType := fieldTypes[i]
		ddl, err := tbl.AddColumnDDL(fn, fieldType)
		if err != nil {
			return err
		}
		dmlLogTbl, err := tbl.ForDriver(ldb.LDBDatabaseDriver)
		if err != nil {
			return err
		}
		logDDL, err := dmlLogTbl.AddColumnDDL(fn, fieldType)
		if err != nil {
			return err
		}
		events.Debug("[CreateTable %{tableName}s] ctldb DDL: %{ddl}s", tableName, ddl)
		events.Debug("[CreateTable %{tableName}s] log DDL: %{ddl}s", tableName, logDDL)
		// create a func here to make rollback semantics a bit easier
		err = func() error {
			tx, err := e.DB.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			dlw := dmlLedgerWriter{
				Tx:        tx,
				TableName: dmlLedgerTableName,
			}
			defer dlw.Close()
			_, err = tx.ExecContext(ctx, ddl)
			if err != nil {
				if strings.Index(err.Error(), "Error 1060:") == 0 || // mysql
					strings.Contains(err.Error(), "duplicate column name") { // sqlite
					return &errs.ConflictError{Err: "Column already exists"}
				}
				return err
			}
			seq, err := dlw.Add(ctx, logDDL)
			if err != nil {
				return err
			}
			err = tx.Commit()
			if err != nil {
				return err
			}
			events.Log("Successfully created new field `%{fieldName}s %{fieldType}v` on table %{tableName}s at seq %{seq}v", fieldName, fieldType, tableName, seq)
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *dbExecutive) GetWriterCookie(writerName string, writerSecret string) ([]byte, error) {
	ctx, cancel := e.ctx()
	defer cancel()

	wn, err := schema.NewWriterName(writerName)
	if err != nil {
		return nil, err
	}

	ms := mutatorStore{
		DB:        e.DB,
		Ctx:       ctx,
		TableName: mutatorsTableName,
	}

	cookie, found, err := ms.Get(wn, writerSecret)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, &errs.NotFoundError{Err: "Writer not found"}
	}

	return cookie, nil
}

func (e *dbExecutive) SetWriterCookie(writerName string, writerSecret string, cookie []byte) error {
	ctx, cancel := e.ctx()
	defer cancel()

	wn, err := schema.NewWriterName(writerName)
	if err != nil {
		return err
	}

	ms := mutatorStore{
		DB:        e.DB,
		Ctx:       ctx,
		TableName: mutatorsTableName,
	}

	err = ms.Update(wn, writerSecret, cookie, nil)

	if err == ErrWriterNotFound {
		return &errs.NotFoundError{Err: err.Error()}
	}
	if err == ErrCookieTooLong {
		return &errs.BadRequestError{Err: err.Error()}
	}

	return err
}

func (e *dbExecutive) HealthCheck() error {
	// TODO: implement actual health checks
	return nil
}

func (e *dbExecutive) Mutate(
	writerName string,
	writerSecret string,
	familyName string,
	cookie []byte,
	checkCookie []byte,
	requests []ExecutiveMutationRequest) error {

	ctx, cancel := e.ctx()
	defer cancel()

	// Reject requests that are too large
	if len(requests) > limits.LimitMaxMutateRequestCount {
		return &errs.PayloadTooLargeError{Err: "Number of requests exceeds maximum"}
	}

	famName, err := schema.NewFamilyName(familyName)
	if err != nil {
		return err
	}

	wn, err := schema.NewWriterName(writerName)
	if err != nil {
		return err
	}

	reqset, err := newMutationRequestSet(famName, requests)
	if err != nil {
		return err
	}

	// Validate table names
	tblNames := reqset.TableNames()
	tbls, err := e.fetchMetaTablesByName(famName, tblNames)
	if err != nil {
		return errors.Wrap(err, "fetch meta tables error")
	}

	for _, tblName := range tblNames {
		if _, ok := tbls[tblName]; !ok {
			return errors.Errorf("Table not found: %s", tblName)
		}
	}

	// Everything is done in a transaction here. This provides the transactional
	// guarantees to the writer, and also allows us to checkpoint the writers
	// cookie data and serialize all accesses by writerName. Transactions are
	// dope, y'all.
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "begin tx error")
	}
	defer tx.Rollback()

	// First check to make sure we can actually make these mutations
	allowed, err := e.limiter.allowed(ctx, tx, limiterRequest{
		writerName: writerName,
		familyName: familyName,
		requests:   requests,
	})
	if err != nil {
		return err
	}
	if !allowed {
		return &errs.RateLimitExceededErr{Err: "rate limit exceeded"}
	}

	// For the ledger to behave as we expect, an exclusive lock needs to be held
	// to prevent anomalies from occurring. There are two primary anomalies that
	// this prevents:
	//
	// 1) Transaction bodies that overlap eachother in the ledger. If more than
	// one mutate batch is being committed to the log at once, without a lock,
	// they will be ordered in such a way that they will overlap in the ledger.
	//
	// 2) Gaps in the transaction log. If Tx1 has taken Seq1 and Tx2 has taken
	// Seq2, and Tx2 commits first, there is a brief point in time where Seq2
	// will exist in the log and Seq1 will not. The Reflector depends on reading
	// the log in sequence order, so rows can't suddenly appear at a previous
	// sequence number AFTER the reflector has already read through that range
	// of the sequence.
	//
	// This statement causes a row-write lock to be held by the database,
	// preventing other writers from interfering, and forcing a linearized
	// order of transactions. This is done in leiu of table locks, which are
	// not part of the SQL standard.
	_, err = tx.ExecContext(ctx, "UPDATE locks SET clock = clock + 1 WHERE id = 'ledger'")
	if err != nil {
		return errors.Wrap(err, "taking ledger lock")
	}

	// Check Cookie
	ms := mutatorStore{
		DB:        tx,
		Ctx:       ctx,
		TableName: mutatorsTableName,
	}

	// If the writer doesn't exist, this will ErrCookieConflict. That is good,
	// because the writer should "create" itself by first calling the
	// GetWriterCookie endpoint.
	err = ms.Update(wn, writerSecret, cookie, checkCookie)
	if err != nil {
		return err
	}

	// Now apply all the requests
	dlw := dmlLedgerWriter{
		Tx:        tx,
		TableName: dmlLedgerTableName,
	}
	defer dlw.Close()

	// To retain transactionality in the log itself, transaction
	// markers must be added into the log. The reflector uses these
	// markers to know when the transaction should be started and
	// committed as it tails the log.
	if len(reqset.Requests) > 1 {
		_, err := dlw.BeginTx(ctx)
		if err != nil {
			return errors.Wrap(err, "logging tx begin failed")
		}
	}

	var lastSeq schema.DMLSequence
	for _, req := range reqset.Requests {
		// TODO: wrap errors in here by request index
		tbl := tbls[req.TableName]

		var values []interface{}
		var dmlSQL string

		// Generate the DML first
		if !req.Delete {
			// UPSERT
			values, err = req.valuesByOrder(tbl.FieldNames())
			if err != nil {
				return err
			}

			dmlSQL, err = tbl.UpsertDML(values)
			if err != nil {
				return err
			}
		} else {
			// DELETE
			values, err = req.valuesByOrder(tbl.KeyFields.Fields)
			if err != nil {
				return err
			}

			dmlSQL, err = tbl.DeleteDML(values)
			if err != nil {
				return err
			}
		}

		if len(dmlSQL) > limits.LimitMaxDMLSize {
			return &errs.BadRequestError{Err: "Request generated too large of a DML statement"}
		}

		// Execute the actual DML write
		_, err = tx.ExecContext(ctx, dmlSQL)
		if err != nil {
			events.Log("dml exec error, Request: %{req}+v SQL: %{sql}s", req, dmlSQL)
			return errors.Wrap(err, "dml exec error")
		}

		// Now record it in the log table
		lastSeq, err = dlw.Add(ctx, dmlSQL)
		if err != nil {
			return errors.Wrap(err, "log write error")
		}
	}

	if len(reqset.Requests) > 1 {
		lastSeq, err = dlw.CommitTx(ctx)
		if err != nil {
			return errors.Wrap(err, "logging tx commit failed")
		}
	}

	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "commit failed")
	}

	events.Debug(
		"Mutate success on family %{familyName}s "+
			"applied %{mutationCount}d mutations "+
			"at seq %{lastSeq}d "+
			"by writer %{writerName}s",
		famName,
		len(requests),
		lastSeq.Int(),
		writerName,
	)

	return nil
}

func (e *dbExecutive) fetchMetaTablesByName(famName schema.FamilyName, tblNames []schema.TableName) (map[schema.TableName]sqlgen.MetaTable, error) {
	ctx, cancel := e.ctx()
	defer cancel()

	if len(tblNames) == 0 {
		return nil, errors.New("fetchMetaTablesByName needs at least one table to fetch")
	}

	encodedTableNames := []string{}
	for _, tblName := range tblNames {
		encodedTableNames = append(encodedTableNames, schema.LDBTableName(famName, tblName))
	}

	dbInfo := getDBInfo(e.DB)
	colInfos, err := dbInfo.GetColumnInfo(ctx, encodedTableNames)
	if err != nil {
		return nil, err
	}

	tbls := map[schema.TableName]sqlgen.MetaTable{}
	driverName := sqlgen.SqlDriverToDriverName(e.DB.Driver())

	var tbl sqlgen.MetaTable
	for _, colInfo := range colInfos {
		tblFamilyName, tblName, err := schema.DecodeLDBTableName(colInfo.TableName)
		if err != nil {
			return nil, err
		}
		if tblFamilyName != famName {
			err = fmt.Errorf("Wow, assertion failed %s != %s", tblFamilyName, famName)
			return nil, err
		}

		// Rows for a table will be contiguous, so switch to the next
		// table when a new table name is encountered.
		if tbl.TableName != tblName {
			if tbl.TableName != schema.TableNameZero {
				// don't copy the empty table on the first pass
				tbls[tbl.TableName] = tbl
			}
			tbl = sqlgen.MetaTable{
				DriverName: driverName,
				FamilyName: tblFamilyName,
				TableName:  tblName,
			}
		}

		ft, _ok := schema.SqlTypeToFieldType(colInfo.DataType)
		if !_ok {
			err = fmt.Errorf("Could not resolve database type: '%s'", colInfo.DataType)
			return nil, err
		}

		fn, err := schema.NewFieldName(colInfo.ColumnName)
		if err != nil {
			return nil, err
		}

		// HERE YOU ARE
		tbl.Fields = append(tbl.Fields, schema.NamedFieldType{Name: fn, FieldType: ft})

		// Magic string that MySQL puts here if this column is part of
		// the primary key
		if colInfo.IsPrimaryKey {
			tbl.KeyFields.Fields = append(tbl.KeyFields.Fields, fn)
		}
	}

	// for loop will exit before "current" table is added to map
	if tbl.TableName != schema.TableNameZero {
		tbls[tbl.TableName] = tbl
	}

	return tbls, nil
}

func (e *dbExecutive) fetchMetaTableByName(famName schema.FamilyName, tblName schema.TableName) (tbl sqlgen.MetaTable, ok bool, err error) {
	tbls, err := e.fetchMetaTablesByName(famName, []schema.TableName{tblName})
	if err != nil {
		return
	}

	tbl, ok = tbls[tblName]
	return
}

// Represents a family as persisted to ctldb
type dbFamily struct {
	ID   int64
	Name string
}

func (e *dbExecutive) fetchFamilyByName(famName schema.FamilyName) (fam dbFamily, ok bool, err error) {
	ctx, cancel := e.ctx()
	defer cancel()

	qs := "SELECT id, name FROM families WHERE name = ?"
	row := e.DB.QueryRowContext(ctx, qs, famName.Name)

	// I dunno why this seems dirty, maybe cuz most langs don't
	// support interior pointers?
	err = row.Scan(&fam.ID, &fam.Name)

	if err == sql.ErrNoRows {
		// not-found returns ([zero-value], false, nil)
		err = nil
		return
	} else if err == nil {
		// No errors, no ErrNoRows, that means found!
		ok = true
	} else {
		// An error!
		err = errors.WithStack(err)
	}

	return
}

func (e *dbExecutive) RegisterWriter(writerName string, secret string) error {
	wn, err := schema.NewWriterName(writerName)
	if err != nil {
		return err
	}

	if len(secret) < limits.LimitWriterSecretMinLength {
		return errors.Errorf("Secret should be at least %d characters", limits.LimitWriterSecretMinLength)
	}
	if len(secret) > limits.LimitWriterSecretMaxLength {
		return errors.Errorf("Secret can be at most %d characters", limits.LimitWriterSecretMaxLength)
	}

	ms := mutatorStore{
		DB:        e.DB,
		Ctx:       e.Ctx,
		TableName: mutatorsTableName,
	}

	return ms.Register(wn, secret)
}

func (e *dbExecutive) ReadRow(familyName string, tableName string, where map[string]interface{}) (map[string]interface{}, error) {
	ctx, cancel := e.ctx()
	defer cancel()

	famName, err := schema.NewFamilyName(familyName)
	if err != nil {
		return nil, &errs.BadRequestError{Err: err.Error()}
	}

	tblName, err := schema.NewTableName(tableName)
	if err != nil {
		return nil, &errs.BadRequestError{Err: err.Error()}
	}

	metaTable, ok, err := e.fetchMetaTableByName(famName, tblName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &errs.NotFoundError{Err: "Table not found"}
	}

	predicate := map[schema.FieldName]interface{}{}
	for fnStr, v := range where {
		fName, err := schema.NewFieldName(fnStr)
		if err != nil {
			return nil, errs.BadRequest("Field name error for '%s': %s", fnStr, err)
		}

		found := false
		for _, keyField := range metaTable.KeyFields.Fields {
			if keyField == fName {
				found = true
				break
			}
		}

		if !found {
			return nil, errs.BadRequest("Predicate contains non-key field: '%s'", fnStr)
		}

		predicate[fName] = v
	}

	for _, keyField := range metaTable.KeyFields.Fields {
		_, ok := predicate[keyField]
		if !ok {
			return nil, errs.BadRequest("Must include all key fields in predicate")
		}
	}

	queryTable := schema.LDBTableName(famName, tblName)
	qs := "SELECT * FROM " + queryTable + " WHERE "
	whereClauseParts := []string{}
	qsArgs := []interface{}{}
	for fldName, val := range predicate {
		whereClauseParts = append(whereClauseParts, fldName.Name+"=?")
		qsArgs = append(qsArgs, val)
	}
	qs = qs + strings.Join(whereClauseParts, " AND ") + " LIMIT 1"

	out := map[string]interface{}{}
	rows, err := e.DB.QueryContext(ctx, qs, qsArgs...)
	if err == sql.ErrNoRows || !rows.Next() {
		rows.Close()
		return out, nil
	}
	if err != nil {
		return nil, err
	}

	cols, err := schema.DBColumnMetaFromRows(rows)
	if err != nil {
		return nil, err
	}

	sfn, err := scanfunc.New(out, cols)
	if err != nil {
		return nil, err
	}

	err = sfn(rows)
	return out, err
}

func (e *dbExecutive) ReadTableSizeLimits() (res limits.TableSizeLimits, err error) {
	ctx, cancel := e.ctx()
	defer cancel()
	res.Global = e.limiter.tableSizer.defaultTableLimit
	rows, err := e.DB.QueryContext(ctx,
		"select family_name, table_name, warn_size_bytes, max_size_bytes "+
			"FROM max_table_sizes "+
			"ORDER BY family_name, table_name")
	if err != nil {
		return res, errors.Wrap(err, "select table sizes")
	}
	defer rows.Close()
	for rows.Next() {
		var tsl limits.TableSizeLimit
		if err := rows.Scan(&tsl.Family, &tsl.Table, &tsl.WarnSize, &tsl.MaxSize); err != nil {
			return res, errors.Wrap(err, "scan table sizes")
		}
		res.Tables = append(res.Tables, tsl)
	}
	return res, rows.Err()
}

func (e *dbExecutive) UpdateTableSizeLimit(limit limits.TableSizeLimit) error {
	ctx, cancel := e.ctx()
	defer cancel()
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "start tx")
	}
	defer tx.Rollback()
	// check first to see if the table exists
	ft := schema.FamilyTable{Family: limit.Family, Table: limit.Table}
	_, err = tx.ExecContext(ctx, "select * from "+ft.String()+" limit 1")
	if err != nil {
		return errors.Errorf("table '%s' not found", ft)
	}
	// then do the upsert
	res, err := tx.ExecContext(ctx, "replace into max_table_sizes "+
		"(family_name, table_name, warn_size_bytes, max_size_bytes) "+
		"values (?, ?, ?, ?)",
		limit.Family, limit.Table, limit.WarnSize, limit.MaxSize)
	if err != nil {
		return errors.Wrap(err, "replace into max_table_sizes")
	}
	ra, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "max_table_sizes rows affected")
	}
	if ra <= 0 {
		return errors.New("unexpected failure -- no rows updated")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "commit tx")
	}
	return nil
}

func (e *dbExecutive) DeleteTableSizeLimit(ft schema.FamilyTable) error {
	ctx, cancel := e.ctx()
	defer cancel()
	events.Log("deleting from max table sizes where f=%v and t=%v", ft.Family, ft.Table)
	resp, err := e.DB.ExecContext(ctx, "delete from max_table_sizes where family_name=? and table_name=?",
		ft.Family, ft.Table)
	if err != nil {
		return errors.Wrap(err, "delete from max_table_sizes")
	}
	rows, err := resp.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "rows affected")
	}
	if rows < 1 {
		return errors.Errorf("could not find table limit for %s", ft)
	}
	return nil
}

func (e *dbExecutive) ReadWriterRateLimits() (res limits.WriterRateLimits, err error) {
	ctx, cancel := e.ctx()
	defer cancel()
	res.Global = e.limiter.defaultWriterLimit
	rows, err := e.DB.QueryContext(ctx,
		"select writer_name, max_rows_per_minute "+
			"FROM max_writer_rates "+
			"ORDER BY writer_name")
	if err != nil {
		return res, errors.Wrap(err, "select writer rates")
	}
	defer rows.Close()
	for rows.Next() {
		var wrl limits.WriterRateLimit
		wrl.RateLimit.Period = time.Minute
		if err := rows.Scan(&wrl.Writer, &wrl.RateLimit.Amount); err != nil {
			return res, errors.Wrap(err, "scan writer rates")
		}
		res.Writers = append(res.Writers, wrl)
	}
	return res, rows.Err()
}

func (e *dbExecutive) UpdateWriterRateLimit(limit limits.WriterRateLimit) error {
	ctx, cancel := e.ctx()
	defer cancel()
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "start tx")
	}
	defer tx.Rollback()

	ms := mutatorStore{DB: e.DB, Ctx: ctx, TableName: mutatorsTableName}
	// the api already verifies that our writer input is good here but we double check b/c
	// computers are fast.
	writer, err := schema.NewWriterName(limit.Writer)
	if err != nil {
		return errors.Wrap(err, "validate writer")
	}
	exists, err := ms.Exists(writer)
	if err != nil {
		return errors.Wrap(err, "check writer exists")
	}
	if !exists {
		return errors.Errorf("no writer with the name '%s' exists", limit.Writer)
	}
	adjustedAmount, err := limit.RateLimit.AdjustAmount(time.Minute)
	if err != nil {
		return errors.Wrap(err, "check limit")
	}
	res, err := tx.ExecContext(ctx, "replace into max_writer_rates "+
		"(writer_name, max_rows_per_minute) "+
		"values (?, ?)", limit.Writer, adjustedAmount)
	if err != nil {
		return errors.Wrap(err, "replace into max_writer_rates")
	}
	ra, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "max_writer_rates rows affected")
	}
	if ra <= 0 {
		return errors.New("unexpected failure -- no rows updated")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "commit tx")
	}
	return nil
}

func (e *dbExecutive) DeleteWriterRateLimit(writerName string) error {
	ctx, cancel := e.ctx()
	defer cancel()
	res, err := e.DB.ExecContext(ctx, "delete from max_writer_rates where writer_name=?", writerName)
	if err != nil {
		return errors.Wrap(err, "delete from max_writer_rates")
	}
	ra, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "get rows affected from max_writer_rates")
	}
	if ra <= 0 {
		return errors.Errorf("no writer limit for the writer '%s' was found", writerName)
	}
	return nil
}

func (e *dbExecutive) DropTable(table schema.FamilyTable) error {
	ctx, cancel := e.ctx()
	defer cancel()

	famName, tblName, tbl, err := sqlgen.BuildMetaTableFromInput(
		sqlgen.SqlDriverToDriverName(e.DB.Driver()),
		table.Family,
		table.Table,
		nil,
		nil,
		nil,
	)
	if err != nil {
		return err
	}
	_, ok, err := e.fetchMetaTableByName(famName, tblName)
	if err != nil {
		return err
	}
	if !ok {
		return errs.NotFound("table %q not found", famName.String()+tblName.String())
	}

	ddl := tbl.DropTableDDL()
	dmlLogTbl, err := tbl.ForDriver(ldb.LDBDatabaseDriver)
	if err != nil {
		return err
	}

	logDDL := dmlLogTbl.DropTableDDL()

	events.Debug("[DropTable %{tableName}s] ctldb DDL: %{ddl}s", table, ddl)
	events.Debug("[DropTable %{tableName}s] log DDL: %{ddl}s", table, logDDL)

	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "error beginning transaction")
	}
	defer tx.Rollback()

	dlw := dmlLedgerWriter{
		Tx:        tx,
		TableName: dmlLedgerTableName,
	}
	defer dlw.Close()

	_, err = tx.ExecContext(ctx, ddl)
	if err != nil {
		return errors.Wrap(err, "error running drop command")
	}

	seq, err := dlw.Add(ctx, logDDL)
	if err != nil {
		return errors.Wrap(err, "error inserting drop command into ledger")
	}

	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "error committing transaction")
	}

	events.Log("Successfully dropped `%{tableName}s` at seq %{seq}v", table.String(), seq)

	return nil
}

func (e *dbExecutive) ClearTable(table schema.FamilyTable) error {
	ctx, cancel := e.ctx()
	defer cancel()

	famName, tblName, tbl, err := sqlgen.BuildMetaTableFromInput(
		sqlgen.SqlDriverToDriverName(e.DB.Driver()),
		table.Family,
		table.Table,
		nil,
		nil,
		nil,
	)
	if err != nil {
		return err
	}
	_, ok, err := e.fetchMetaTableByName(famName, tblName)
	if err != nil {
		return err
	}
	if !ok {
		return errs.NotFound("table %q not found", famName.String()+tblName.String())
	}
	e.fetchMetaTableByName(famName, tblName)

	ddl := tbl.ClearTableDDL()
	dmlLogTbl, err := tbl.ForDriver(ldb.LDBDatabaseDriver)
	if err != nil {
		return err
	}

	logDDL := dmlLogTbl.ClearTableDDL()

	events.Debug("[ClearTable %{tableName}s] ctldb DDL: %{ddl}s", table, ddl)
	events.Debug("[ClearTable %{tableName}s] log DDL: %{ddl}s", table, logDDL)

	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "error beginning transaction")
	}
	defer tx.Rollback()

	dlw := dmlLedgerWriter{
		Tx:        tx,
		TableName: dmlLedgerTableName,
	}
	defer dlw.Close()

	_, err = tx.ExecContext(ctx, ddl)
	if err != nil {
		return errors.Wrap(err, "error running delete command")
	}

	seq, err := dlw.Add(ctx, logDDL)
	if err != nil {
		return errors.Wrap(err, "error inserting delete command into ledger")
	}

	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "error committing transaction")
	}

	events.Log("Successfully deleted all rows from `%{tableName}s` at seq %{seq}v", table.String(), seq)

	return nil
}

func (e *dbExecutive) ReadFamilyTableNames(family schema.FamilyName) (tables []schema.FamilyTable, err error) {
	ctx, cancel := e.ctx()
	defer cancel()

	events.Debug("reading family table names where f=%s", family)
	rows, err := e.DB.QueryContext(ctx, fmt.Sprintf(`select table_name from information_schema.tables where table_name like '%s___%%'`, family.String()))
	if err != nil {
		return nil, errors.Wrap(err, "error reading family table names")
	}
	defer rows.Close()
	for rows.Next() {
		var fullTableName string
		if err := rows.Scan(&fullTableName); err != nil {
			return nil, errors.Wrap(err, "error reading family table names")
		}
		prefix := family.String() + "___"
		table := strings.TrimPrefix(fullTableName, prefix)
		ft := schema.FamilyTable{
			Family: family.String(),
			Table:  table,
		}
		tables = append(tables, ft)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return tables, nil
}

func sanitizeFamilyAndTableNames(family string, table string) (string, string, error) {
	sanFamily, err := schema.NewFamilyName(family)
	if err != nil {
		return "", "", errors.Wrap(err, "sanitize family")
	}
	sanTable, err := schema.NewTableName(table)
	if err != nil {
		return "", "", errors.Wrap(err, "sanitize table")
	}
	return sanFamily.Name, sanTable.Name, nil
}
