package ldbwriter

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/schema"
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

func TestCheckpointQuery(t *testing.T) {
	db, teardown := ldb.LDBForTest(t)
	defer teardown()
	defer db.Close()
	writer := SqlLdbWriter{Db: db}
	err := writer.ApplyDMLStatement(context.Background(), schema.NewTestDMLStatement("CREATE TABLE foo (bar VARCHAR);"))
	if err != nil {
		t.Fatalf("Could not issue CREATE TABLE statement, error %v", err)
	}

	tests := []struct {
		cpType CheckpointType
	}{
		{
			cpType: Full,
		},
		{
			cpType: Passive,
		},
		{
			cpType: Restart,
		},
		{
			cpType: Truncate,
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.cpType), func(t *testing.T) {
			res, err := writer.Checkpoint(tt.cpType)
			err = writer.ApplyDMLStatement(context.Background(), schema.NewTestDMLStatement("INSERT INTO foo VALUES('hello');"))
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if res.Busy == 1 {
				t.Errorf("expected busy to be 0, got %d", res.Busy)
			}

			if tt.cpType != Truncate && res.Checkpointed <= 0 {
				t.Errorf("expected Checkpointed to be greater than 0, got %d", res.Checkpointed)
			}

			if res.Checkpointed != res.Log {
				t.Errorf("expected checkpointed and log to be equal, got %v", res)
			}
		})
	}
}

// SimulateBusyCheckpointing this reproduces blocked checkpointing by having lots of readers with long queries open
func TestSimulateBusyCheckpointing(t *testing.T) {
	db, teardown, path := ldb.LDBForTestWithPath(t)
	defer teardown()
	defer db.Close()
	writer := SqlLdbWriter{Db: db}
	writer.ApplyDMLStatement(context.Background(), schema.NewTestDMLStatement("CREATE TABLE foo (bar VARCHAR);"))

	var waiter sync.WaitGroup

	fi, _ := os.Stat(path + "-wal")
	fmt.Printf("Path: %s-wal\n", path)
	fmt.Printf("File size: %d\n", fi.Size())

	readers := 35
	writers := 5
	waiter.Add(readers + writers)
	tester := func(x string) {
		defer waiter.Done()
		for i := 0; i < 20_000; i++ {
			time.Sleep(1 * time.Millisecond)
			writer.ApplyDMLStatement(context.Background(), schema.NewTestDMLStatement(fmt.Sprintf("INSERT INTO foo VALUES('%s%d');", x, i)))
		}
	}

	cper := func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				s := time.Now()
				res, _ := writer.Checkpoint(Truncate)
				if res.Busy == 1 {
					fmt.Println("Checkpoint was busy")
				}
				e := time.Now().Sub(s)
				if e.Seconds() > 0.5 {
					fmt.Printf("Checkpoint took: %fs\n", e.Seconds())
				}
			}
		}
	}

	readTest := func() {
		time.Sleep(5 * time.Second)
		defer waiter.Done()
		res, err := db.Query("SELECT * FROM foo LIMIT 1000")
		if err != nil {
			fmt.Println(err)
			return
		}

		for res.Next() {
			var val string
			res.Scan(&val)
			time.Sleep(20 * time.Millisecond)
		}
	}

	go cper()
	for i := 0; i < writers; i++ {
		go tester(fmt.Sprintf("%d-abcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefghabcdefgh", i))
	}

	for i := 0; i < readers; i++ {
		go readTest()
	}

	waiter.Wait()

	fi, _ = os.Stat(path + "-wal")
	fmt.Printf("File size: %d\n", fi.Size())

}
