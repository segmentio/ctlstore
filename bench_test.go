package ctlstore

import (
	"context"
	"database/sql"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/tests"
)

func BenchmarkLDBQueryBaseline(b *testing.B) {
	ctx := context.TODO()

	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatalf("Unexpected error creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ldbPath := filepath.Join(tmpDir, "tmp.db")
	localDB, err := sql.Open("sqlite3", ldbPath)
	if err != nil {
		b.Fatalf("Unexpected error opening LDB: %v", err)
	}
	defer localDB.Close()

	err = ldb.EnsureLdbInitialized(ctx, localDB)
	if err != nil {
		b.Fatalf("Unexpected error initializing LDB: %v", err)
	}

	_, err = localDB.ExecContext(ctx, `
		CREATE TABLE foo___bar (
			key VARCHAR PRIMARY KEY,
			val VARCHAR
		);
		INSERT INTO foo___bar VALUES('foo', 'bar');
	`)
	if err != nil {
		b.Fatalf("Unexpected error inserting value into LDB: %v", err)
	}

	prepQ, err := localDB.PrepareContext(ctx, "SELECT * FROM foo___bar WHERE key = ?")
	if err != nil {
		b.Fatalf("Unexpected error preparing query: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := prepQ.QueryContext(ctx, "foo")
		if err != nil {
			b.Errorf("Error querying: %v", err)
			continue
		}
		for rows.Next() {
			var keyReceiver, valReceiver string
			err = rows.Scan(&keyReceiver, &valReceiver)
			if err != nil {
				b.Errorf("Error scanning: %v", err)
				continue
			}
			if keyReceiver != "foo" {
				b.Errorf("Received unexpected key: %v", keyReceiver)
			}
			if valReceiver != "bar" {
				b.Errorf("Received unexpected val: %v", valReceiver)
			}
		}
	}
}

func BenchmarkGetRowByKey(b *testing.B) {
	ctx := context.TODO()

	type benchContext struct {
		ldb *sql.DB
		ctx context.Context
		r   *LDBReader
	}

	type benchKVRow struct {
		Key   string `ctlstore:"key"`
		Value string `ctlstore:"val"`
	}

	testTmpDir, teardown := tests.WithTmpDir(b)
	defer teardown()

	ldbPath := filepath.Join(testTmpDir, "tmp.db")
	localDB, err := sql.Open("sqlite3", ldbPath)
	if err != nil {
		b.Fatalf("Unexpected error opening LDB: %v", err)
	}

	err = ldb.EnsureLdbInitialized(ctx, localDB)
	if err != nil {
		b.Fatalf("Unexpected error initializing LDB: %v", err)
	}

	_, err = localDB.ExecContext(ctx, `
			CREATE TABLE foo___bar (
				key VARCHAR PRIMARY KEY,
				val VARCHAR
			);
			INSERT INTO foo___bar VALUES('foo', 'bar');
		`)
	if err != nil {
		b.Fatalf("Unexpected error inserting value into LDB: %v", err)
	}

	r := NewLDBReaderFromDB(localDB)

	benchSetup := &benchContext{
		ldb: localDB,
		ctx: ctx,
		r:   r,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var row benchKVRow
		found, err := benchSetup.r.GetRowByKey(benchSetup.ctx, &row, "foo", "bar", "foo")
		if err != nil {
			b.Fatalf("Unexpected error calling GetRowByKey: %v", err)
		}
		if !found {
			b.Fatal("Should have found a row")
		}
		if row.Key != "foo" {
			b.Fatalf("Unexpected value in row key: %v", row.Key)
		}
		if row.Value != "bar" {
			b.Fatalf("Unexpected value in row val: %v", row.Value)
		}
	}
}
