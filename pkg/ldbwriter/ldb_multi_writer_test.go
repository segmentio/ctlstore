package ldbwriter

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/schema"
)

func TestMultiApplyDMLStatement(t *testing.T) {
	var err error
	dbA, teardown1 := ldb.LDBForTest(t)
	defer teardown1()

	dbB, teardown2 := ldb.LDBForTest(t)
	defer teardown2()

	dbC, teardown3 := ldb.LDBForTest(t)
	defer teardown3()
	ctx := context.Background()

	aWriter := SqlLdbWriter{Db: dbA}
	bWriter := SqlLdbWriter{Db: dbB}
	cWriter := SqlLdbWriter{Db: dbC}

	mw := NewMultiWriter(&aWriter, &bWriter, &cWriter)

	lastLedgerUpdate := func(db *sql.DB) (time.Time, error) {
		row := db.QueryRowContext(ctx, "select timestamp from "+ldb.LDBLastUpdateTableName+" where name=?", ldb.LDBLastLedgerUpdateColumn)
		var timestamp time.Time
		err := row.Scan(&timestamp)
		return timestamp, err
	}

	_, err = lastLedgerUpdate(dbA)
	require.Equal(t, sql.ErrNoRows, err)
	_, err = lastLedgerUpdate(dbB)
	require.Equal(t, sql.ErrNoRows, err)
	_, err = lastLedgerUpdate(dbC)
	require.Equal(t, sql.ErrNoRows, err)

	err = mw.ApplyDMLStatement(ctx, schema.NewTestDMLStatement("CREATE TABLE foo (bar VARCHAR);"))
	if err != nil {
		t.Fatalf("Could not issue CREATE TABLE statement, error %v", err)
	}

	tsA1, err := lastLedgerUpdate(dbA)
	require.NoError(t, err)
	require.True(t, time.Now().Sub(tsA1) > 0, "invalid timestamp: %v", tsA1)
	tsB1, err := lastLedgerUpdate(dbB)
	require.NoError(t, err)
	require.True(t, time.Now().Sub(tsB1) > 0, "invalid timestamp: %v", tsB1)
	tsC1, err := lastLedgerUpdate(dbC)
	require.NoError(t, err)
	require.True(t, time.Now().Sub(tsC1) > 0, "invalid timestamp: %v", tsC1)

	err = mw.ApplyDMLStatement(ctx, schema.NewTestDMLStatement("INSERT INTO foo VALUES('hello');"))
	if err != nil {
		t.Errorf("Could not issue INSERT statement, error %v", err)
	}

	tsA2, err := lastLedgerUpdate(dbA)
	require.NoError(t, err)
	require.True(t, tsA2.UnixNano() >= tsA1.UnixNano())
	tsB2, err := lastLedgerUpdate(dbB)
	require.NoError(t, err)
	require.True(t, tsB2.UnixNano() >= tsB1.UnixNano())
	tsC2, err := lastLedgerUpdate(dbC)
	require.NoError(t, err)
	require.True(t, tsC2.UnixNano() >= tsC1.UnixNano())

	for _, db := range []*sql.DB{dbA, dbB, dbC} {
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
}

func TestMultiWriteError(t *testing.T) {

	dbA, teardown1 := ldb.LDBForTest(t)
	defer teardown1()

	dbB, teardown2 := ldb.LDBForTest(t)
	defer teardown2()

	dbC, teardown3 := ldb.LDBForTest(t)
	defer teardown3()
	ctx := context.Background()

	aWriter := SqlLdbWriter{Db: dbA}
	bWriter := SqlLdbWriter{Db: dbB}
	cWriter := SqlLdbWriter{Db: dbC}

	mw := NewMultiWriter(&aWriter, &bWriter, &cWriter)

	err := mw.ApplyDMLStatement(ctx, schema.NewTestDMLStatement("INSERT INTO foo VALUES('hello');"))
	if err == nil {
		t.Fatalf("Error should not be nil %v", err)
	}

	if err.Error() != `exec dml statement error: no such table: foo
exec dml statement error: no such table: foo
exec dml statement error: no such table: foo` {
		t.Errorf("Error was not as expected: %v", err)
	}

}
