package ctlstore

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlgen"
)

// NewLDBTestUtil changes the global default LDB path to a temporary path.
//
// This function is NOT concurrency safe.
func NewLDBTestUtil(t testing.TB) (*LDBTestUtil, func()) {
	tmpDir, err := ioutil.TempDir("", "ldb_test")
	if err != nil {
		t.Fatal(err)
	}

	globalLDBDirPath = tmpDir
	path := filepath.Join(tmpDir, ldb.DefaultLDBFilename)
	globalLDBReadOnly = false
	globalReader = nil

	db, err := sql.Open(ldb.LDBDatabaseDriver, fmt.Sprintf(
		"file:%s?_journal_mode=wal&mode=%s&cache=shared",
		path,
		"rwc",
	))
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatal(err)
	}

	err = ldb.EnsureLdbInitialized(context.Background(), db)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatal(err)
	}

	tu := &LDBTestUtil{DB: db, T: t}
	return tu, func() {
		os.RemoveAll(tmpDir)
	}
}

// NewLDBTestUtilLocal is just like NewLDBTestUtil above except it does not rely
// on global state and is therefore threadsafe, at the cost of requiring users
// to use ensure that the DB property is used to initialize the ctlstore Reader
// instead of relying on the global/default init.
func NewLDBTestUtilLocal(t testing.TB) (*LDBTestUtil, func()) {
	tmpDir, err := ioutil.TempDir("", "ldb_test")
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(tmpDir, ldb.DefaultLDBFilename)

	db, err := sql.Open(ldb.LDBDatabaseDriver, fmt.Sprintf(
		"file:%s?_journal_mode=wal&mode=%s&cache=shared",
		path,
		"rwc",
	))
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatal(err)
	}

	err = ldb.EnsureLdbInitialized(context.Background(), db)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatal(err)
	}

	tu := &LDBTestUtil{DB: db, T: t}
	return tu, func() {
		os.RemoveAll(tmpDir)
	}
}

// LDBTestUtil provides basic unit testing facilities for injecting data
// into a "fake" LDB.
type LDBTestUtil struct {
	DB *sql.DB
	T  testing.TB
}

// LDBTestTableDef is used to pass a table definition to CreateTable
// for use in tests that need the LDB. The way the parameters are
// specified mimics the executive interface. Fields are passed as
// tuples of [name string, type string] where type can be something
// like "string" or "integer," just like the standard executive
// interface.
type LDBTestTableDef struct {
	Family    string
	Name      string
	Fields    [][]string
	KeyFields []string
	Rows      [][]interface{}
}

// CreateTable creates a table in the target test LDB.
func (tu *LDBTestUtil) CreateTable(def LDBTestTableDef) {
	fns, fts, err := schema.UnzipFieldsParam(def.Fields)
	if err != nil {
		tu.T.Fatalf("Error unziping field params: %+v", err)
	}

	_, _, tbl, err := sqlgen.BuildMetaTableFromInput(
		sqlgen.SqlDriverToDriverName(tu.DB.Driver()),
		def.Family,
		def.Name,
		fns,
		fts,
		def.KeyFields,
	)

	if err != nil {
		tu.T.Fatalf("Error building table def from input: %+v", err)
	}

	err = tbl.Validate()
	if err != nil {
		tu.T.Fatalf("Table validation error: %+v", err)
	}

	ddl, err := tbl.AsCreateTableDDL()
	if err != nil {
		tu.T.Fatalf("Error rendering table DDL: %+v", err)
	}

	tx, err := tu.DB.BeginTx(context.Background(), nil)
	if err != nil {
		tu.T.Fatalf("Error beginning DDL tx: %+v", err)
	}

	_, err = tx.Exec(ddl)
	if err != nil {
		tu.T.Fatalf("Error executing DDL: %+v", err)
	}

	err = tx.Commit()
	if err != nil {
		tu.T.Fatalf("Error committing DDL tx: %+v", err)
	}

	if def.Rows != nil {
		tu.InsertRows(def.Family, def.Name, def.Rows)
	}
}

// InsertRows well, inserts rows into the LDB. Rows are passed as
// tuples in the table's column order.
func (tu *LDBTestUtil) InsertRows(family string, table string, rows [][]interface{}) {
	hunks := []string{
		"INSERT INTO",
		fmt.Sprintf("%s___%s", family, table),
		"VALUES",
	}
	params := []interface{}{}

	for ri, row := range rows {
		if ri > 0 {
			hunks = append(hunks, ",")
		}
		hunks = append(hunks, "(")
		for vi, val := range row {
			if vi > 0 {
				hunks = append(hunks, ",")
			}
			hunks = append(hunks, "?")
			params = append(params, val)
		}
		hunks = append(hunks, ")")
	}

	qs := strings.Join(hunks, " ")
	_, err := tu.DB.Exec(qs, params...)
	if err != nil {
		tu.T.Fatalf("Unexpected error inserting data: %+v", err)
	}

	qs = fmt.Sprintf(
		"REPLACE INTO %s (name, timestamp) VALUES (?, ?)",
		ldb.LDBLastUpdateTableName,
	)
	_, err = tu.DB.Exec(qs, ldb.LDBLastLedgerUpdateColumn, time.Now())
	if err != nil {
		tu.T.Fatalf("Unexpected error updating ledger timestamp: %+v", err)
	}
}

// DeleteAll deletes all rows from the given table.
func (tu *LDBTestUtil) DeleteAll(family string, table string) {
	hunks := []string{
		"DELETE FROM",
		fmt.Sprintf("%s___%s", family, table),
	}

	qs := strings.Join(hunks, " ")
	_, err := tu.DB.Exec(qs)
	if err != nil {
		tu.T.Fatalf("Unexpected error deleting data: %+v", err)
	}
}

// Reset completely clears the test LDB
func (tu *LDBTestUtil) Reset() {
	qs := "SELECT DISTINCT tbl_name FROM sqlite_master"
	tbls, err := tu.DB.Query(qs)
	if err != nil {
		tu.T.Fatalf("Unexpected error querying table list: %+v", err)
	}

	tblNames := []string{}
	for tbls.Next() {
		var name string
		err = tbls.Scan(&name)
		if err != nil {
			tu.T.Fatalf("Unexpected error scanning table list: %+v", err)
		}
		if !strings.HasPrefix(name, "_") {
			tblNames = append(tblNames, name)
		}
	}

	for _, name := range tblNames {
		qs := fmt.Sprintf("DROP TABLE '%s'", name)
		_, err := tu.DB.Exec(qs)
		if err != nil {
			tu.T.Fatalf("Unexpected error dropping table: %+v", err)
		}
	}
}
