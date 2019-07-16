package reflector

import (
	"context"
	"database/sql"
	"testing"

	"github.com/segmentio/ctlstore"
	ldb2 "github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/ldbwriter"
)

// Exercises the basic components of the DML source and LDB writer/reader
func TestPipelineIntegration(t *testing.T) {
	var err error
	ctx := context.Background()
	ldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Couldn't open LDB, error: %+v", err)
	}
	ctldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Couldn't open ctldb, error: %+v", err)
	}

	err = ldb2.EnsureLdbInitialized(ctx, ldb)
	if err != nil {
		t.Fatalf("Couldn't initialize LDB, error: %+v", err)
	}

	srcutil := &sqlDmlSourceTestUtil{db: ctldb, t: t}
	srcutil.InitializeDB()
	dmlsrc := &sqlDmlSource{db: ctldb, ledgerTableName: "ctlstore_dml_ledger"}

	ldbw := ldbwriter.SqlLdbWriter{Db: ldb}
	ldbr := ctlstore.NewLDBReaderFromDB(ldb)

	applyAllStatements := func() {
		for {
			st, err := dmlsrc.Next(ctx)
			if err == errNoNewStatements {
				return
			} else if err != nil {
				t.Fatalf("error reading statements from DML source, error: %+v", err)
			}
			ldbw.ApplyDMLStatement(ctx, st)
		}
	}

	srcutil.AddStatement("CREATE TABLE foo___bar (key VARCHAR PRIMARY KEY, val VARCHAR)")
	applyAllStatements()

	row := struct {
		Key string `ctlstore:"key"`
		Val string `ctlstore:"val"`
	}{}

	{
		found, err := ldbr.GetRowByKey(ctx, &row, "foo", "bar", "zzz")
		if err != nil {
			t.Errorf("Unexpected error reading from LDB: %+v", err)
		}
		if found {
			t.Error("Expected to not find any rows before we INSERT something")
		}
	}

	srcutil.AddStatement("INSERT INTO foo___bar VALUES('zzz', 'yyy')")
	applyAllStatements()

	{
		_, err := ldbr.GetRowByKey(ctx, &row, "foo", "bar", "zzz")
		if err != nil {
			t.Errorf("Unexpected error reading from LDB: %+v", err)
		}
		if row.Key != "zzz" {
			t.Errorf("Unexpected row key %+v", row.Key)
		}
		if row.Val != "yyy" {
			t.Errorf("Unexpected row val %+v", row.Val)
		}
	}
}
