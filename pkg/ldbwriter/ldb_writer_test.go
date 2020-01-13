package ldbwriter

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/stretchr/testify/require"
)

var testLdbSeq int

func TestApplyDMLStatement(t *testing.T) {
	var err error
	db, teardown := ldb.LDBForTest(t)
	defer teardown()
	ctx := context.Background()
	writer := SqlLdbWriter{Db: db}

	lastLedgerUpdate := func() (time.Time, error) {
		row := db.QueryRowContext(ctx, "select timestamp from "+ldb.LDBLastUpdateTableName+" where name=?", ldb.LDBLastLedgerUpdateColumn)
		var timestamp time.Time
		err := row.Scan(&timestamp)
		return timestamp, err
	}

	_, err = lastLedgerUpdate()
	require.Equal(t, sql.ErrNoRows, err)

	err = writer.ApplyDMLStatement(ctx, schema.NewTestDMLStatement("CREATE TABLE foo (bar VARCHAR);"))
	if err != nil {
		t.Fatalf("Could not issue CREATE TABLE statement, error %v", err)
	}

	ts1, err := lastLedgerUpdate()
	require.NoError(t, err)
	require.True(t, time.Now().Sub(ts1) > 0, "invalid timestamp: %v", ts1)

	err = writer.ApplyDMLStatement(ctx, schema.NewTestDMLStatement("INSERT INTO foo VALUES('hello');"))
	if err != nil {
		t.Errorf("Could not issue INSERT statement, error %v", err)
	}

	ts2, err := lastLedgerUpdate()
	require.NoError(t, err)
	require.True(t, ts2.UnixNano() >= ts1.UnixNano())

	rows, err := db.Query("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("Could not SELECT from test table, error %v", err)
	}
	defer rows.Close()

	rowCount := 0
	rowData := []string{}
	var rowTmp string

	for rows.Next() {
		err = rows.Scan(&rowTmp)
		rowCount++
		rowData = append(rowData, rowTmp)
	}

	if rowCount != 1 {
		t.Errorf("Row count differs, got %v, expected 1", rowCount)
	} else {
		if rowData[0] != "hello" { // matches value from INSERT
			t.Errorf("Data in row differs (uhhh), got %v, expected %v", rowData[0], "hello")
		}
	}
}

func TestApplyDMLStatementMonotonic(t *testing.T) {
	var err error

	db, teardown := ldb.LDBForTest(t)
	defer teardown()
	ctx := context.Background()
	writer := SqlLdbWriter{Db: db}

	err = writer.ApplyDMLStatement(ctx, schema.DMLStatement{
		Statement: "SELECT 1;",
		Sequence:  schema.DMLSequence(100),
	})
	if err != nil {
		t.Errorf("Couldn't apply DML statement, error: %v", err)
	}

	fetchedSeq, err := ldb.FetchSeqFromLdb(ctx, db)
	if err != nil {
		// Fatal here cuz this test is now pretty suspect because of default values
		t.Fatalf("Couldn't fetch tracking sequence from LDB, error: %v", err)
	}

	if fetchedSeq != schema.DMLSequence(100) {
		t.Errorf("Seq should be 100, but is %v", fetchedSeq)
	}

	// now we'll try to rewind the seq and see if it is monotonic!
	err = writer.ApplyDMLStatement(ctx, schema.DMLStatement{
		Statement: "SELECT 1;",
		Sequence:  schema.DMLSequence(50),
	})
	if err == nil {
		t.Fatalf("Expected error, got %v", err)
	} else if want, got := "update seq tracker replay detected", err.Error(); want != got {
		t.Errorf("Expected %v, got %v", want, got)
	}

	// Use another var cuz if we reuse this it can be a false positive
	fetchedSeq2, err := ldb.FetchSeqFromLdb(ctx, db)
	if err != nil {
		t.Errorf("Couldn't fetch tracking sequence from LDB, error: %v", err)
	}

	if fetchedSeq2 != schema.DMLSequence(100) {
		t.Errorf("Seq should be 100, but is %v", fetchedSeq2)
	}
}

func TestApplyDMLStatementTransaction(t *testing.T) {
	var err error
	db, teardown := ldb.LDBForTest(t)
	defer teardown()
	ctx := context.Background()
	writer := SqlLdbWriter{Db: db}

	err = writer.ApplyDMLStatement(ctx, schema.NewTestDMLStatement("CREATE TABLE foo (bar VARCHAR);"))
	if err != nil {
		t.Fatalf("Could not issue CREATE TABLE statement, error %v", err)
	}

	// going straight to the DB means it'll be in another TX, so
	// isolation should apply.
	var cnt int64
	row := db.QueryRow("SELECT COUNT(*) FROM foo")
	err = row.Scan(&cnt)
	if err != nil {
		t.Errorf("[a] Couldn't scan table, error %v", err)
	}

	if cnt != 0 {
		t.Errorf("Table should be empty")
	}

	err = writer.ApplyDMLStatement(ctx, schema.NewTestDMLStatement(schema.DMLTxBeginKey))
	if err != nil {
		t.Fatalf("Could not apply BEGIN TX statement, error %v", err)
	}

	err = writer.ApplyDMLStatement(ctx, schema.NewTestDMLStatement("INSERT INTO foo VALUES('hello');"))
	if err != nil {
		t.Errorf("Could not issue INSERT statement, error %v", err)
	}

	// going straight to the DB means it'll be in another TX, so
	// isolation should apply.
	row = db.QueryRow("SELECT COUNT(*) FROM foo")
	err = row.Scan(&cnt)
	if err != nil {
		t.Errorf("[b]Couldn't scan table, error %v", err)
	}

	if cnt != 0 {
		t.Errorf("Transactionality violation")
	}

	err = writer.ApplyDMLStatement(ctx, schema.NewTestDMLStatement(schema.DMLTxEndKey))
	if err != nil {
		t.Fatalf("Could not apply COMMIT TX statement, error %v", err)
	}

	rows, err := db.Query("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("Could not SELECT from test table, error %v", err)
	}
	defer rows.Close()

	rowCount := 0
	rowData := []string{}
	var rowTmp string

	for rows.Next() {
		err = rows.Scan(&rowTmp)
		rowCount++
		rowData = append(rowData, rowTmp)
	}

	if rowCount != 1 {
		t.Errorf("Row count differs, got %v, expected 1", rowCount)
	} else {
		if rowData[0] != "hello" { // matches value from INSERT
			t.Errorf("Data in row differs (uhhh), got %v, expected %v", rowData[0], "hello")
		}
	}
}

func TestApplyDMLStatementAlreadyOpenTxFails(t *testing.T) {
	var err error
	db, teardown := ldb.LDBForTest(t)
	defer teardown()
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}
	defer tx.Rollback()

	writer := SqlLdbWriter{Db: db, LedgerTx: tx}
	err = writer.ApplyDMLStatement(ctx, schema.NewTestDMLStatement(schema.DMLTxBeginKey))
	if err == nil {
		t.Errorf("Expected error, got nil")
	} else if want, got := "invariant violation", err.Error(); want != got {
		t.Errorf("Expected: %v, got %v", want, got)
	}
}

func TestApplyDMLStatementAlreadyClosedTxFails(t *testing.T) {
	var err error
	db, teardown := ldb.LDBForTest(t)
	defer teardown()
	defer db.Close()
	writer := SqlLdbWriter{Db: db}
	ctx := context.Background()

	err = writer.ApplyDMLStatement(ctx, schema.NewTestDMLStatement(schema.DMLTxEndKey))
	if err == nil {
		t.Errorf("Expected error, got nil")
	} else if want, got := "invariant violation", err.Error(); want != got {
		t.Errorf("Expected: %v, got %v", want, got)
	}
}

func TestApplyDMLStatementLedgerBeginTxAdvancesSeq(t *testing.T) {
	var err error
	db, teardown := ldb.LDBForTest(t)
	defer teardown()
	defer db.Close()
	writer := SqlLdbWriter{Db: db}
	ctx := context.Background()

	stmt := schema.NewTestDMLStatement(schema.DMLTxBeginKey)
	err = writer.ApplyDMLStatement(ctx, stmt)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}

	row := writer.LedgerTx.QueryRow(
		fmt.Sprintf("SELECT seq FROM %s WHERE id = ?", ldb.LDBSeqTableName),
		ldb.LDBSeqTableID)

	var seq int
	err = row.Scan(&seq)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}
	if want, got := seq, int(stmt.Sequence.Int()); want != got {
		t.Errorf("Expected: %v, got %v", want, got)
	}
}

func TestApplyDMLStatementLedgerEndTxAdvancesSeq(t *testing.T) {
	var err error
	db, teardown := ldb.LDBForTest(t)
	defer teardown()
	defer db.Close()
	writer := SqlLdbWriter{Db: db}
	ctx := context.Background()

	stmt1 := schema.NewTestDMLStatement(schema.DMLTxBeginKey)
	err = writer.ApplyDMLStatement(ctx, stmt1)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}

	stmt2 := schema.NewTestDMLStatement(schema.DMLTxEndKey)
	err = writer.ApplyDMLStatement(ctx, stmt2)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}

	row := writer.Db.QueryRow(
		fmt.Sprintf("SELECT seq FROM %s WHERE id = ?", ldb.LDBSeqTableName),
		ldb.LDBSeqTableID)

	var seq int
	err = row.Scan(&seq)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}
	if want, got := seq, int(stmt2.Sequence.Int()); want != got {
		t.Errorf("Expected: %v, got %v", want, got)
	}
}

func TestApplyDMLStatementReplayAborts(t *testing.T) {
	var err error
	db, teardown := ldb.LDBForTest(t)
	defer teardown()
	defer db.Close()
	writer := SqlLdbWriter{Db: db}
	ctx := context.Background()

	// Using a CREATE TABLE statement on purpose here, so that it
	// will ensure replay is caught before it is applied, since
	// a CREATE TABLE isn't idempotent (it will gen an error)
	stmt := schema.NewTestDMLStatement("CREATE TABLE foo (bar VARCHAR);")
	err = writer.ApplyDMLStatement(ctx, stmt)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}

	err = writer.ApplyDMLStatement(ctx, stmt)
	if err == nil {
		t.Fatalf("Expected error, got %v", err)
	} else if want, got := "update seq tracker replay detected", err.Error(); want != got {
		t.Errorf("Expected %v, got %v", want, got)
	}
}
