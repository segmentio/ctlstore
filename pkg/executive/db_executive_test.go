/*
 *
 * IMPORTANT: All of the tests for dbExecutive are called from the
 * TestAllDBExecutive() function, which runs tests thru both the
 * SQLite and the MySQL code paths. Use lowercase t in your test
 * function name and add it to the map in TestAllDBExecutive to
 * get it to run thru both.
 *
 */

package executive

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/ctldb"
	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlgen"
	"github.com/segmentio/ctlstore/pkg/tests"
	"github.com/segmentio/ctlstore/pkg/units"
	"github.com/segmentio/ctlstore/pkg/utils"
	"github.com/stretchr/testify/require"
)

type dbExecTestFn func(*testing.T, string)

const (
	// Schema executed to initialize the test database.
	testCtlDBSchemaUpForMySQL = `
		CREATE TABLE family1___table1 (
			field1 BIGINT,
			field2 VARCHAR(4000),
			field3 DOUBLE
		);

		CREATE TABLE family1___table100 (
			field1 BIGINT,
			field2 VARCHAR(4000),
			field3 DOUBLE
		);

		CREATE TABLE family1___table10 (
			field1 BIGINT PRIMARY KEY,
			field2 VARCHAR(4000),
			field3 DOUBLE
		);

		CREATE TABLE family1___table11 (
			field1 BIGINT PRIMARY KEY,
			field2 VARCHAR(4000),
			field3 DOUBLE
		); 

		CREATE TABLE family1___binary_table1 (
			field1 BIGINT PRIMARY KEY,
			field2 VARBINARY(1000)
		); 
		`

	testCtlDBSchemaUpForSQLite3 = `
		CREATE TABLE family1___table1 (
			field1 INTEGER,
			field2 VARCHAR,
			field3 REAL
		);

		CREATE TABLE family1___table100 (
			field1 INTEGER,
			field2 VARCHAR,
			field3 REAL
		);

		CREATE TABLE family1___table11 (
			field1 INTEGER PRIMARY KEY,
			field2 VARCHAR,
			field3 REAL
		);

		CREATE TABLE family1___table10 (
			field1 INTEGER PRIMARY KEY,
			field2 VARCHAR,
			field3 REAL
		); 

		CREATE TABLE family1___binary_table1 (
			field1 INTEGER PRIMARY KEY,
			field2 BLOB
		); 
		`

	testCtlDBUpSQL = `
		INSERT INTO family1___table10 VALUES(1, 'foo', 1.2);
		INSERT INTO mutators (writer, secret, cookie) VALUES('writer1', 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', x'01');
		INSERT INTO families (id, name) VALUES(1, 'family1');
`
)

// these are the table/writer limits used as defaults during testing.
var (
	testDefaultTableLimit  = limits.SizeLimits{MaxSize: 10 * units.MEGABYTE, WarnSize: 5 * units.MEGABYTE}
	testDefaultWriterLimit = limits.RateLimit{Amount: 1000, Period: time.Minute}
)

func TestAllDBExecutive(t *testing.T) {
	dbTypes := []string{"mysql", "sqlite3"}
	testFns := map[string]dbExecTestFn{
		"testDBExecutiveCreateFamily":         testDBExecutiveCreateFamily,
		"testDBExecutiveCreateTable":          testDBExecutiveCreateTable,
		"testDBExecutiveAddFields":            testDBExecutiveAddFields,
		"testDBExecutiveFetchFamilyByName":    testDBExecutiveFetchFamilyByName,
		"testDBExecutiveMutate":               testDBExecutiveMutate,
		"testDBExecutiveGetWriterCookie":      testDBExecutiveGetWriterCookie,
		"testDBExecutiveSetWriterCookie":      testDBExecutiveSetWriterCookie,
		"testFetchMetaTableByName":            testFetchMetaTableByName,
		"testDBExecutiveRegisterWriter":       testDBExecutiveRegisterWriter,
		"testDBExecutiveReadRow":              testDBExecutiveReadRow,
		"testDBLimiter":                       testDBLimiter,
		"testDBExecutiveWriterRates":          testDBExecutiveWriterRates,
		"testDBExecutiveTableLimits":          testDBExecutiveTableLimits,
		"testDBExecutiveClearTable":           testDBExecutiveClearTable,
		"testDBExecutiveDropTable":            testDBExecutiveDropTable,
		"testDBExecutiveReadFamilyTableNames": testDBExecutiveReadFamilyTableNames,
	}

	for _, dbType := range dbTypes {
		for testName, testFn := range testFns {
			t.Run(testName+"_"+dbType, func(t *testing.T) {
				testFn(t, dbType)
			})
		}
	}
}

func newCtlDBTestConnection(t *testing.T, dbType string) (*sql.DB, func()) {
	var (
		teardowns utils.Teardowns
		db        *sql.DB
		schemaUp  string
		err       error
	)
	switch dbType {
	case "mysql":
		schemaUp = testCtlDBSchemaUpForMySQL
		db, err = sql.Open("mysql", ctldb.GetTestCtlDBDSN(t))
		if err != nil {
			t.Fatalf("Can't connect to local MySQL: %+v", err)
		}
		teardowns.Add(func() { db.Close() })

		rows, err := db.Query(
			`SELECT table_name FROM information_schema.tables
			WHERE table_schema = DATABASE()`)
		if err != nil {
			t.Fatalf("Query error: %+v", err)
		}
		defer rows.Close()

		dropDDL := []string{}
		for rows.Next() {
			var tableName string
			err = rows.Scan(&tableName)
			if err != nil {
				t.Fatalf("Scan error: %+v", err)
			}
			dropDDL = append(dropDDL, fmt.Sprintf("DROP TABLE %s;", tableName))
		}

		// Protects us from execing an empty statement, which will error
		if len(dropDDL) > 0 {
			for _, ddl := range dropDDL {
				_, err = db.Exec(ddl)
				if err != nil {
					t.Fatalf("Error exec'ing drop DDL: %v", err)
				}
			}
		}
	case "sqlite3":
		schemaUp = testCtlDBSchemaUpForSQLite3
		tmpDir, td := tests.WithTmpDir(t)
		teardowns.Add(td)
		db, err = sql.Open("sqlite3", filepath.Join(tmpDir, "ctldb.db"))
	default:
		t.Fatalf("unknown dbtype %q", dbType)
	}

	err = ctldb.InitializeCtlDB(db, sqlgen.SqlDriverToDriverName)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}

	schemaUp += testCtlDBUpSQL
	statements := strings.Split(schemaUp, ";")

	for _, statement := range statements {
		tsql := strings.TrimSpace(statement)
		if tsql == "" {
			continue
		}
		_, err := db.Exec(tsql)
		if err != nil {
			t.Fatalf("While executing %s, unexpected error: %+v", tsql, err)
		}
	}

	return db, teardowns.Teardown
}

type dbExecTestUtil struct {
	db       *sql.DB
	t        *testing.T
	e        *dbExecutive
	ctx      context.Context
	teardown func()
}

func (tu *dbExecTestUtil) Close() error {
	err := tu.db.Close()
	tu.teardown()
	return err
}

func newDbExecTestUtil(t *testing.T, dbType string) *dbExecTestUtil {
	ctx := context.Background()
	db, teardown := newCtlDBTestConnection(t, dbType)

	// TODO: review size limits and constraints on ctldb
	limiter := newDBLimiter(db, dbType, testDefaultTableLimit, testDefaultWriterLimit.Period, testDefaultWriterLimit.Amount)
	dbe := dbExecutive{DB: db, Ctx: ctx, limiter: limiter}

	return &dbExecTestUtil{
		db:       db,
		t:        t,
		ctx:      ctx,
		e:        &dbe,
		teardown: teardown,
	}
}

func testDBExecutiveCreateFamily(t *testing.T, dbType string) {
	u := newDbExecTestUtil(t, dbType)
	err := u.e.CreateFamily("family2")
	if err != nil {
		t.Errorf("Unexpected error calling CreateFamily: %+v", err)
	}

	row := u.db.QueryRow("SELECT COUNT(*) FROM families WHERE name = 'family2'")
	var cnt sql.NullInt64
	err = row.Scan(&cnt)
	if err != nil {
		t.Fatalf("Unexpected error scanning result: %v", err)
	}

	if want, got := 1, cnt; !got.Valid || int(got.Int64) != want {
		t.Errorf("Expected %v rows, got %v", want, got)
	}

	err = u.e.CreateFamily("family2")
	if err != nil && err.Error() != "Family already exists" {
		t.Errorf("Unexpected error %v", err)
	}
}

func testDBExecutiveRegisterWriter(t *testing.T, dbType string) {
	u := newDbExecTestUtil(t, dbType)
	ms := mutatorStore{DB: u.db, Ctx: u.ctx, TableName: mutatorsTableName}

	// first ensure that register writer succeeds
	err := u.e.RegisterWriter("writerTest", "secret1")
	require.NoError(t, err)

	_, found, err := ms.Get(schema.WriterName{Name: "writerTest"}, "secret1")
	require.NoError(t, err)
	require.True(t, found)

	// try to register again with the same credentials
	err = u.e.RegisterWriter("writerTest", "secret1")
	require.NoError(t, err)

	// register the same writer but with a different credential
	err = u.e.RegisterWriter("writerTest", "some new secret")
	require.Equal(t, err, ErrWriterAlreadyExists)
}

func queryDMLTable(t *testing.T, db *sql.DB, limit int) []string {
	sql := "select statement from ctlstore_dml_ledger order by seq desc"
	if limit > 0 {
		sql += " limit " + strconv.Itoa(limit)
	}
	stRows, err := db.Query("select statement from ctlstore_dml_ledger order by seq desc limit 6")
	require.NoError(t, err)
	var statements []string
	for stRows.Next() {
		var statement string
		err := stRows.Scan(&statement)
		require.NoError(t, err)
		statements = append(statements, statement)
	}
	require.NoError(t, stRows.Err())
	return statements
}

func testDBExecutiveAddFields(t *testing.T, dbType string) {
	u := newDbExecTestUtil(t, dbType)
	defer u.Close()

	addFields := func() error {
		return u.e.AddFields("family1",
			"table2",
			[]string{"field7", "field8", "field9", "field10", "field11", "field12"},
			[]schema.FieldType{schema.FTString, schema.FTInteger, schema.FTByteString, schema.FTDecimal, schema.FTText, schema.FTBinary},
		)
	}

	// first verify that we cannot add to the table if it does not already exist.
	err := addFields()
	require.Error(t, err)
	// also verify that no DML exists
	dmls := queryDMLTable(t, u.db, -1)
	require.Empty(t, dmls)

	err = u.e.CreateTable("family1",
		"table2",
		[]string{"field1", "field2", "field3", "field4", "field5", "field6"},
		[]schema.FieldType{schema.FTString, schema.FTInteger, schema.FTByteString, schema.FTDecimal, schema.FTText, schema.FTBinary},
		[]string{"field1", "field2", "field3"},
	)
	if err != nil {
		t.Fatalf("Unexpected error calling CreateTable: %+v", err)
	}

	err = addFields()
	if err != nil {
		t.Fatalf("Unexpected error calling UpdateTable: %+v", err)
	}

	// ensure that the table was modified correctly in the ctldb

	res, err := u.db.Exec(`INSERT into family1___table2
		(field1,field2,field3,field4,field5,field6,field7,field8,field9,field10,field11,field12)
		VALUES	('1',2,'3',4.1,'5',x'6a','7',8,'9',10.1,'11',x'12') `)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if rows != int64(1) {
		t.Fatal(rows)
	}

	// ensure that the DML was added to the ledger
	statements := queryDMLTable(t, u.db, 6)
	require.EqualValues(t, []string{
		"ALTER TABLE family1___table2 ADD COLUMN \"field12\" BLOB",
		"ALTER TABLE family1___table2 ADD COLUMN \"field11\" TEXT",
		"ALTER TABLE family1___table2 ADD COLUMN \"field10\" REAL",
		"ALTER TABLE family1___table2 ADD COLUMN \"field9\" BLOB(255)",
		"ALTER TABLE family1___table2 ADD COLUMN \"field8\" INTEGER",
		"ALTER TABLE family1___table2 ADD COLUMN \"field7\" VARCHAR(191)",
	}, statements)

	err = u.e.AddFields("family1",
		"table2",
		[]string{"field7", "field8", "field9", "field10", "field11", "field12"},
		[]schema.FieldType{schema.FTString, schema.FTInteger, schema.FTByteString, schema.FTDecimal, schema.FTText, schema.FTBinary},
	)
	if err == nil || !strings.Contains(err.Error(), "Column already exists") {
		t.Fatalf("Unexpected error calling UpdateTable: %+v", err)
	}
}

func testDBExecutiveCreateTable(t *testing.T, dbType string) {
	u := newDbExecTestUtil(t, dbType)
	defer u.Close()

	createTable := func() error {
		return u.e.CreateTable("family1",
			"table2",
			[]string{"field1", "field2", "field3", "field4", "field5", "field6"},
			[]schema.FieldType{schema.FTString, schema.FTInteger, schema.FTByteString, schema.FTDecimal, schema.FTText, schema.FTBinary},
			[]string{"field1", "field2", "field3"},
		)
	}
	err := createTable()
	require.NoError(t, err)
	dmls := queryDMLTable(t, u.db, -1)
	require.Len(t, dmls, 1) // one DML should exist to create the table

	// try to create the table again, verify it fails, and verify that the ledger is correct
	err = createTable()
	require.Error(t, err)
	dmls = queryDMLTable(t, u.db, -1)
	require.Len(t, dmls, 1) // there should still only be one DML

	// Just check that an empty table exists at all, because the field
	// creation logic gets checked by sqlgen unit tests
	row := u.db.QueryRow("SELECT COUNT(*) FROM family1___table2")

	var cnt sql.NullInt64
	err = row.Scan(&cnt)
	if err != nil {
		t.Fatalf("Unexpected error scanning result: %+v", err)
	}

	if want, got := 0, cnt; !got.Valid || int(got.Int64) != want {
		t.Errorf("Expected %+v, got %+v", want, got)
	}

	logRow := u.db.QueryRow("SELECT statement FROM " + dmlLedgerTableName)
	var rowStatement string
	err = logRow.Scan(&rowStatement)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}

	indexOfCreate := strings.Index(rowStatement, "CREATE TABLE family1___table2")
	if want, got := 0, indexOfCreate; want != got {
		t.Errorf("Expected %+v, got %+v", want, got)
	}

	err = u.e.CreateTable("family1",
		"table2",
		[]string{"field1", "field2", "field3"},
		[]schema.FieldType{schema.FTString, schema.FTInteger, schema.FTDecimal},
		[]string{"field1"},
	)
	if err == nil || err.Error() != "Table already exists" {
		t.Errorf("Unexpected error calling CreateTable: %+v", err)
	}

	err = u.e.CreateTable("family1",
		"table3",
		[]string{"field1", "field2"},
		[]schema.FieldType{schema.FTString, schema.FTInteger},
		[]string{"field3"})
	if err == nil || err.Error() != "Primary key field 'field3' not specified as a field" {
		t.Errorf("Unexpected error calling CreateTable: %+v", err)
	}

	err = u.e.CreateTable("family1",
		"table4",
		[]string{"field1", "field2"},
		[]schema.FieldType{schema.FTString, schema.FTDecimal},
		[]string{"field2"})
	if err == nil || err.Error() != "Fields of type 'decimal' cannot be a key field" {
		t.Errorf("Unexpected error calling CreateTable: %+v", err)
	}

	err = u.e.CreateTable("family1",
		"table4",
		[]string{"field1", "field2"},
		[]schema.FieldType{schema.FTString, schema.FTDecimal},
		[]string{})
	if err == nil || err.Error() != "table must have at least one key field" {
		t.Errorf("Unexpected error calling CreateTable: %+v", err)
	}
}

func testDBExecutiveTableLimits(t *testing.T, dbType string) {
	u := newDbExecTestUtil(t, dbType)
	defer u.Close()

	ctx, cancel := context.WithCancel(u.ctx)
	defer cancel()

	// assert that there are not table limits
	tsLimits, err := u.e.ReadTableSizeLimits()
	require.NoError(t, err)
	require.EqualValues(t, limits.TableSizeLimits{Global: testDefaultTableLimit}, tsLimits)

	tableLimit1 := limits.TableSizeLimit{
		Family: "foo",
		Table:  "bar",
		SizeLimits: limits.SizeLimits{
			MaxSize:  100,
			WarnSize: 5,
		},
	}
	tableLimit2 := limits.TableSizeLimit{
		Family: "foo2",
		Table:  "baz",
		SizeLimits: limits.SizeLimits{
			MaxSize:  1100,
			WarnSize: 15,
		},
	}

	// ensure that you can't set table size limits for tables that do not exist
	err = u.e.UpdateTableSizeLimit(tableLimit1)
	require.EqualError(t, errors.Cause(err), "table 'foo___bar' not found")

	// createTable creates a table in the ctldb with a generic schema
	createTable := func(family, name string) {
		require.NoError(t, u.e.CreateFamily(family))
		fieldNames := []string{"name", "data"}
		fieldTypes := []schema.FieldType{schema.FTString, schema.FTBinary}
		keyFields := []string{"name"}
		err = u.e.CreateTable(family, name, fieldNames, fieldTypes, keyFields)
		require.NoError(t, err)
	}

	// create the table
	createTable("foo", "bar")

	// then the mutation to set a table size limit should work
	err = u.e.UpdateTableSizeLimit(tableLimit1)
	require.NoError(t, err)

	// verify that the mutation exists
	tsLimits, err = u.e.ReadTableSizeLimits()
	require.NoError(t, err)
	require.EqualValues(t, testDefaultTableLimit, tsLimits.Global)
	require.EqualValues(t, []limits.TableSizeLimit{tableLimit1}, tsLimits.Tables)

	// verify that the mutation exists in the table that the limiter expects
	var warnSize, maxSize int64
	row := u.e.DB.QueryRowContext(ctx, "select warn_size_bytes, max_size_bytes "+
		"from max_table_sizes "+
		"where family_name=? and table_name=?", tableLimit1.Family, tableLimit1.Table)
	err = row.Scan(&warnSize, &maxSize)
	require.NoError(t, err)
	require.EqualValues(t, tableLimit1.WarnSize, warnSize)
	require.EqualValues(t, tableLimit1.MaxSize, maxSize)

	// create another table limit, but we will also create a new table first
	createTable(tableLimit2.Family, tableLimit2.Table)
	err = u.e.UpdateTableSizeLimit(tableLimit2)
	require.NoError(t, err)

	// verify that it shows up in the table limit query
	tsLimits, err = u.e.ReadTableSizeLimits()
	require.NoError(t, err)
	require.EqualValues(t, testDefaultTableLimit, tsLimits.Global)
	require.EqualValues(t, []limits.TableSizeLimit{tableLimit1, tableLimit2}, tsLimits.Tables)

	// delete the first table limit
	err = u.e.DeleteTableSizeLimit(schema.FamilyTable{Family: tableLimit1.Family, Table: tableLimit1.Table})
	require.NoError(t, err)

	// verify it no longer exists
	tsLimits, err = u.e.ReadTableSizeLimits()
	require.NoError(t, err)
	require.EqualValues(t, testDefaultTableLimit, tsLimits.Global)
	require.EqualValues(t, []limits.TableSizeLimit{tableLimit2}, tsLimits.Tables)

	// update the second table limit to a different value
	tableLimit2.MaxSize = 5000000
	require.NoError(t, u.e.UpdateTableSizeLimit(tableLimit2))

	// verify that the value was updated
	tsLimits, err = u.e.ReadTableSizeLimits()
	require.NoError(t, err)
	require.EqualValues(t, testDefaultTableLimit, tsLimits.Global)
	require.EqualValues(t, []limits.TableSizeLimit{tableLimit2}, tsLimits.Tables)
}

func testDBExecutiveWriterRates(t *testing.T, dbType string) {
	u := newDbExecTestUtil(t, dbType)
	defer u.Close()

	ctx, cancel := context.WithCancel(u.ctx)
	defer cancel()

	// assert that there are no writer limits
	wrLimits, err := u.e.ReadWriterRateLimits()
	require.NoError(t, err)
	require.EqualValues(t, testDefaultWriterLimit, wrLimits.Global)
	require.Len(t, wrLimits.Writers, 0)

	const (
		writer1 = "my-writer-1"
		writer2 = "my-writer-2"
	)
	writerLimit1 := limits.WriterRateLimit{
		Writer: writer1,
		RateLimit: limits.RateLimit{
			Amount: 2,
			Period: time.Second,
		},
	}
	writerLimit2 := limits.WriterRateLimit{
		Writer: writer2,
		RateLimit: limits.RateLimit{
			Amount: 120,
			Period: time.Minute,
		},
	}
	// the limiter converts all rates to the configured period (1m). note
	// that this is not needed for writerLimit2 because it's already based
	// on the configured period.
	expectedWriterLimit1 := limits.WriterRateLimit{
		Writer: writer1,
		RateLimit: limits.RateLimit{
			Amount: 120,
			Period: time.Minute,
		},
	}

	// verify the writer must first exist
	err = u.e.UpdateWriterRateLimit(writerLimit1)
	require.EqualError(t, err, "no writer with the name '"+writer1+"' exists")

	require.NoError(t, u.e.RegisterWriter(writer1, "my-writer-secret"))
	err = u.e.UpdateWriterRateLimit(writerLimit1)
	require.NoError(t, err)

	// verify that the writer appears now in a read request
	wrLimits, err = u.e.ReadWriterRateLimits()
	require.NoError(t, err)
	require.EqualValues(t, testDefaultWriterLimit, wrLimits.Global)
	require.EqualValues(t, []limits.WriterRateLimit{expectedWriterLimit1}, wrLimits.Writers)

	// verify that the limit exists in the table that the limiter reads as well
	row := u.db.QueryRowContext(ctx, "select max_rows_per_minute "+
		"from max_writer_rates "+
		"where writer_name=?", writer1)
	var value int64
	require.NoError(t, row.Scan(&value))
	require.EqualValues(t, 120, value)

	// create another writer limit
	require.NoError(t, u.e.RegisterWriter(writer2, "my-writer-secret-2"))
	err = u.e.UpdateWriterRateLimit(writerLimit2)
	require.NoError(t, err)

	// verify it shows up in the rate limit read query
	wrLimits, err = u.e.ReadWriterRateLimits()
	require.NoError(t, err)
	require.EqualValues(t, testDefaultWriterLimit, wrLimits.Global)
	require.EqualValues(t, []limits.WriterRateLimit{expectedWriterLimit1, writerLimit2}, wrLimits.Writers)

	// delete the first writer limit
	require.NoError(t, u.e.DeleteWriterRateLimit(writer1))

	// verify it no longer exists
	wrLimits, err = u.e.ReadWriterRateLimits()
	require.NoError(t, err)
	require.EqualValues(t, testDefaultWriterLimit, wrLimits.Global)
	require.EqualValues(t, []limits.WriterRateLimit{writerLimit2}, wrLimits.Writers)

	// update the second writer limit to a different value
	writerLimit2.RateLimit.Amount = 300
	require.NoError(t, u.e.UpdateWriterRateLimit(writerLimit2))

	// verify that the value was updated
	wrLimits, err = u.e.ReadWriterRateLimits()
	require.NoError(t, err)
	require.EqualValues(t, testDefaultWriterLimit, wrLimits.Global)
	require.EqualValues(t, []limits.WriterRateLimit{writerLimit2}, wrLimits.Writers)
}

func testDBExecutiveFetchFamilyByName(t *testing.T, dbType string) {
	// Table testing this is so overkill, I get it. I just can't write
	// software without intermediate unit tests. I'm too stupid.
	suite := []struct {
		desc       string
		familyName string
		wantFam    dbFamily
		wantOk     bool
		wantErr    string
	}{
		{"Found case", "family1", dbFamily{1, "family1"}, true, ""},
		{"Not found", "family2", dbFamily{}, false, ""},
		{"Error", "family1", dbFamily{}, false, "sql: database is closed"},
	}

	for i, testCase := range suite {
		testName := fmt.Sprintf("[%d] %s", i, testCase.desc)
		t.Run(testName, func(t *testing.T) {
			u := newDbExecTestUtil(t, dbType)
			defer u.Close()

			if strings.Contains(strings.ToLower(testCase.desc), "error") {
				// I hate this so much
				u.db.Close()
			}

			famName, err := schema.NewFamilyName(testCase.familyName)
			if err != nil {
				t.Fatalf("Family name %s invalid: %+v", testCase.familyName, err)
			}
			fam, ok, err := u.e.fetchFamilyByName(famName)

			// Supreme Go-l0rd bmizerany told me to use cmp
			if diff := cmp.Diff(testCase.wantFam, fam); diff != "" {
				t.Errorf("returned dbFamily differs\n%s", diff)
			}
			if diff := cmp.Diff(testCase.wantOk, ok); diff != "" {
				t.Errorf("returned ok differs\n%s", diff)
			}

			// error I'm looking for isn't exported, damnit
			if want, got := testCase.wantErr, err; true {
				if got == nil {
					if want != "" {
						t.Errorf("Expected no error returned, got %+v", got)
					}
				} else {
					if want != got.Error() {
						t.Errorf("Expected: %+v, got %+v\n", want, got)
					}
				}
			}
		})
	}
}

func testFetchMetaTableByName(t *testing.T, dbType string) {
	suite := []struct {
		desc       string
		familyName string
		tableName  string
		wantFields []schema.NamedFieldType
		wantPK     []string
		wantOk     bool
		wantErr    error
	}{
		{"Found case",
			"family1",
			"table1",
			[]schema.NamedFieldType{
				{Name: schema.FieldName{Name: "field1"}, FieldType: schema.FTInteger},
				{Name: schema.FieldName{Name: "field2"}, FieldType: schema.FTString},
				{Name: schema.FieldName{Name: "field3"}, FieldType: schema.FTDecimal},
			},
			[]string{},
			true,
			nil,
		},
	}

	for i, testCase := range suite {
		testName := fmt.Sprintf("[%d] %s", i, testCase.desc)
		t.Run(testName, func(t *testing.T) {
			u := newDbExecTestUtil(t, dbType)
			defer u.Close()

			famName, err := schema.NewFamilyName(testCase.familyName)
			if err != nil {
				t.Fatalf("Invalid family name %s, error: %+v", famName, err)
			}
			tblName, err := schema.NewTableName(testCase.tableName)
			if err != nil {
				t.Fatalf("Invalid table name %s, error: %+v", tblName, err)
			}

			tbl, gotOk, gotErr := u.e.fetchMetaTableByName(famName, tblName)

			if got, want := tbl.FamilyName.String(), testCase.familyName; got != want {
				t.Errorf("Expected %+v, got %+v", want, got)
			}

			if got, want := tbl.TableName.String(), testCase.tableName; got != want {
				t.Errorf("Expected %+v, got %+v", want, got)
			}

			if diff := cmp.Diff(testCase.wantFields, tbl.Fields); diff != "" {
				t.Errorf("returned dbFamily differs\n%s", diff)
			}

			if got, want := gotOk, testCase.wantOk; got != want {
				t.Errorf("Expected %+v, got %+v", want, got)
			}

			if got, want := gotErr, testCase.wantErr; got != want {
				t.Errorf("Expected %+v, got %+v", want, got)
			}
		})
	}
}

func testDBExecutiveMutate(t *testing.T, dbType string) {
	suite := []struct {
		desc        string
		writerName  string
		cookie      []byte
		checkCookie []byte
		reqs        []ExecutiveMutationRequest
		expectErr   error
		expectRows  map[string][]map[string]interface{}
		expectDML   []string
		skipDBTypes []string
	}{
		{
			desc:        "MySQL String Column With Null Value",
			skipDBTypes: []string{"sqlite3"}, // sqlite3 cannot retrieve this data without truncating so we skip it as a backend
			reqs: []ExecutiveMutationRequest{
				{
					TableName: "table1",
					Delete:    false,
					Values: map[string]interface{}{
						"field1": 1,
						"field2": "a\u0000b",
						"field3": 42,
					},
				},
			},
			expectRows: map[string][]map[string]interface{}{
				"table1": {
					{"field1": 1},
				},
			},
			expectDML: []string{
				`REPLACE INTO family1___table1 ("field1","field2","field3") ` +
					`VALUES(1,x'610062',42)`,
			},
		},
		{
			desc: "Binary Column Null Value",
			reqs: []ExecutiveMutationRequest{
				{
					TableName: "binary_table1",
					Delete:    false,
					Values: map[string]interface{}{
						"field1": 1,
						"field2": nil,
					},
				},
			},
			expectRows: map[string][]map[string]interface{}{
				"binary_table1": {
					{"field1": 1},
				},
			},
			expectDML: []string{
				`REPLACE INTO family1___binary_table1 ("field1","field2") ` +
					`VALUES(1,NULL)`,
			},
		},
		{
			desc: "Empty Table Insert",
			reqs: []ExecutiveMutationRequest{
				{
					TableName: "table1",
					Delete:    false,
					Values: map[string]interface{}{
						"field1": 1,
						"field2": "bar",
						"field3": 10.0,
					},
				},
			},
			expectRows: map[string][]map[string]interface{}{
				"table1": {
					{"field1": 1, "field2": "bar", "field3": 10.0},
				},
			},
			expectDML: []string{
				`REPLACE INTO family1___table1 ("field1","field2","field3") ` +
					`VALUES(1,'bar',10)`,
			},
		},
		{
			desc: "Unicode Insert",
			reqs: []ExecutiveMutationRequest{
				{
					TableName: "table1",
					Delete:    false,
					Values: map[string]interface{}{
						"field1": 1,
						"field2": "𨅝",
						"field3": 10.0,
					},
				},
			},
			expectRows: map[string][]map[string]interface{}{
				"table1": {
					{"field1": 1, "field2": "𨅝", "field3": 10.0},
				},
			},
			expectDML: []string{
				`REPLACE INTO family1___table1 ("field1","field2","field3") ` +
					`VALUES(1,'𨅝',10)`,
			},
		},
		{
			desc: "Escaped String Insert",
			reqs: []ExecutiveMutationRequest{
				{
					TableName: "table1",
					Delete:    false,
					Values: map[string]interface{}{
						"field1": 1,
						"field2": `\\d`,
						"field3": 10.0,
					},
				},
			},
			expectRows: map[string][]map[string]interface{}{
				"table1": {
					{"field1": 1, "field2": `\\d`, "field3": 10.0},
				},
			},
			expectDML: []string{
				`REPLACE INTO family1___table1 ("field1","field2","field3") ` +
					`VALUES(1,'\\d',10)`,
			},
		},
		{
			desc: "Multi Insert",
			reqs: []ExecutiveMutationRequest{
				{
					TableName: "table1",
					Delete:    false,
					Values: map[string]interface{}{
						"field1": 1,
						"field2": "bar",
						"field3": 10.0,
					},
				},
				{
					TableName: "table100",
					Delete:    false,
					Values: map[string]interface{}{
						"field1": 2,
						"field2": "baz",
						"field3": 20.0,
					},
				},
			},
			expectRows: map[string][]map[string]interface{}{
				"table1": {
					{"field1": 1, "field2": "bar", "field3": 10.0},
				},
				"table100": {
					{"field1": 2, "field2": "baz", "field3": 20.0},
				},
			},
			expectDML: []string{
				schema.DMLTxBeginKey,
				`REPLACE INTO family1___table1 ("field1","field2","field3") ` +
					`VALUES(1,'bar',10)`,
				`REPLACE INTO family1___table100 ("field1","field2","field3") ` +
					`VALUES(2,'baz',20)`,
				schema.DMLTxEndKey,
			},
		},
		{
			desc: "Delete",
			reqs: []ExecutiveMutationRequest{
				{
					TableName: "table10",
					Delete:    true,
					Values: map[string]interface{}{
						"field1": 1,
					},
				},
				{
					TableName: "table11",
					Delete:    true,
					Values: map[string]interface{}{
						"field1": 1,
					},
				},
			},
			expectRows: map[string][]map[string]interface{}{
				"table10": {},
				"table11": {},
			},
			expectDML: []string{
				schema.DMLTxBeginKey,
				`DELETE FROM family1___table10 WHERE "field1" = 1`,
				`DELETE FROM family1___table11 WHERE "field1" = 1`,
				schema.DMLTxEndKey,
			},
		},
		{
			desc: "Null Value Insert",
			reqs: []ExecutiveMutationRequest{
				{
					TableName: "table1",
					Delete:    false,
					Values: map[string]interface{}{
						"field1": 1,
						"field2": nil,
						"field3": 10.0,
					},
				},
			},
			// expectRows doesn't work here for some reason, but the
			// statement is fine.
			expectDML: []string{
				`REPLACE INTO family1___table1 ("field1","field2","field3") ` +
					`VALUES(1,NULL,10)`,
			},
		},
		{
			desc: "Replace row in table",
			reqs: []ExecutiveMutationRequest{
				{
					TableName: "table10",
					Delete:    false,
					Values: map[string]interface{}{
						"field1": 1,
						"field2": "bar",
						"field3": 0.0,
					},
				},
			},
			expectRows: map[string][]map[string]interface{}{
				"table10": {
					{"field1": 1, "field2": "bar", "field3": 0.0},
				},
			},
			expectDML: []string{
				`REPLACE INTO family1___table10 ("field1","field2","field3") ` +
					`VALUES(1,'bar',0)`,
			},
		},
		{
			desc: "Error on missing fields",
			reqs: []ExecutiveMutationRequest{
				{
					TableName: "table1",
					Delete:    false,
					Values: map[string]interface{}{
						"field1": 1,
						"field2": "bar",
					},
				},
			},
			expectErr: errors.New("Missing field field3"),
			expectRows: map[string][]map[string]interface{}{
				"table1": {},
			},
		},
		{
			desc: "Check cookie correct",
			reqs: []ExecutiveMutationRequest{
				{
					TableName: "table1",
					Delete:    false,
					Values: map[string]interface{}{
						"field1": 1,
						"field2": "bar",
						"field3": 10.0,
					},
				},
			},
			cookie:      []byte{2},
			checkCookie: []byte{1},
		},
		{
			desc: "No check cookie succeeds",
			reqs: []ExecutiveMutationRequest{
				{
					TableName: "table1",
					Delete:    false,
					Values: map[string]interface{}{
						"field1": 1,
						"field2": "bar",
						"field3": 10.0,
					},
				},
			},
			cookie: []byte{2},
		},
		{
			desc: "Max DML Size Exceeded",
			reqs: []ExecutiveMutationRequest{
				{
					TableName: "table1",
					Delete:    false,
					Values: map[string]interface{}{
						"field1": 1,
						"field2": strings.Repeat("b", 769*units.KILOBYTE),
						"field3": 10.0,
					},
				},
			},
			expectErr: &errs.BadRequestError{Err: "Request generated too large of a DML statement"},
		},
	}

	for caseIdx, testCase := range suite {
		var skipTest bool
		for _, skipDBType := range testCase.skipDBTypes {
			if skipDBType == dbType {
				skipTest = true
			}
		}
		if skipTest {
			continue
		}

		testName := fmt.Sprintf("[%d] %s", caseIdx, testCase.desc)
		t.Run(testName, func(t *testing.T) {
			u := newDbExecTestUtil(t, dbType)
			defer u.Close()

			writerName := testCase.writerName
			if writerName == "" {
				writerName = "writer1"
			}

			cookie := testCase.cookie
			if cookie == nil {
				cookie = []byte{2}
			}

			err := u.e.Mutate(writerName, "", "family1", cookie, testCase.checkCookie, testCase.reqs)

			if err != nil {
				if testCase.expectErr != nil {
					if want, got := testCase.expectErr, err; want != got && want.Error() != got.Error() {
						t.Errorf("Expected error %+v, got %+v", want, got)
					}
				} else {
					t.Errorf("Unexpected error: %+v", err)
				}
			} else {
				if testCase.expectErr != nil {
					t.Errorf("Expected error: %+v, got nil", testCase.expectErr)
				}
			}

			if testCase.expectDML != nil {
				rows, err := u.db.Query(
					"SELECT statement FROM " +
						dmlLedgerTableName +
						" ORDER BY seq ASC")

				if err != nil {
					t.Errorf("Unexpected error: %+v", err)
				} else {
					defer rows.Close()
					i := 0
					for rows.Next() {
						var rowStatement string
						err = rows.Scan(&rowStatement)
						if err != nil {
							t.Fatalf("Unexpected error scanning: %+v", err)
						}
						if i < len(testCase.expectDML) {
							if want, got := testCase.expectDML[i], rowStatement; want != got {
								t.Errorf("Expected %+v, got %+v", want, got)
							}
						} else {
							t.Errorf("Extra statement: %v", rowStatement)
						}
						i++
					}
				}
			}

			if testCase.expectRows != nil {
				for name, rows := range testCase.expectRows {
					famName, _ := schema.NewFamilyName("family1")
					tblName, err := schema.NewTableName(name)
					if err != nil {
						t.Fatalf("Invalid table name %s, error: %+v", name, err)
					}

					sqlTableName := schema.LDBTableName(famName, tblName)

					var rowCnt int
					cntRow := u.db.QueryRow("SELECT COUNT(*) FROM " + sqlTableName)
					err = cntRow.Scan(&rowCnt)
					if err != nil {
						t.Fatalf("Unexpected error encountered: %+v", err)
					}

					if want, got := len(rows), rowCnt; want != got {
						t.Errorf("Expected %s to have %d rows, got %d", sqlTableName, want, got)
					}

					for _, row := range rows {
						clauses := []string{}
						valz := []interface{}{}
						for colName, colVal := range row {
							clauses = append(clauses, colName+"=?")
							valz = append(valz, colVal)
						}
						whereClause := " WHERE " + strings.Join(clauses, " AND ")

						var cnt int
						qs := "SELECT COUNT(*) FROM " + sqlTableName + whereClause
						t.Logf("Running %s", qs)
						resRow := u.db.QueryRow(qs, valz...)
						err = resRow.Scan(&cnt)
						if err != nil {
							t.Fatalf("Unexpected error encountered: %+v", err)
						}

						if cnt != 1 {
							t.Errorf("Expected to find 1 row of %+v, got: %+v", row, cnt)
						}
					}
				}
			}
		})
	}
}

func testDBExecutiveGetWriterCookie(t *testing.T, dbType string) {
	suite := []struct {
		desc         string
		writerName   string
		writerSecret string
		expectCookie []byte
		expectErr    string
	}{
		{
			desc:       "Empty writer returns error",
			writerName: "writer-doesnt-exist",
			expectErr:  "Writer not found",
		},
		{
			desc:         "Bad writer secret returns error",
			writerName:   "writer1",
			writerSecret: "invalid",
			expectErr:    "Writer not found",
		},
		{
			desc:         "Existing writer",
			writerName:   "writer1",
			expectCookie: []byte{1},
		},
	}

	for _, testCase := range suite {
		t.Run(testCase.desc, func(t *testing.T) {
			u := newDbExecTestUtil(t, dbType)
			defer u.Close()

			gotCookie, gotErr := u.e.GetWriterCookie(testCase.writerName, testCase.writerSecret)

			if diff := cmp.Diff(testCase.expectCookie, gotCookie); diff != "" {
				t.Errorf("Cookie differs\n%s", diff)
			}
			if want, got := testCase.expectErr, gotErr; (got == nil && want != "") || (got != nil && want != got.Error()) {
				t.Errorf("Expected: %v, got %v", want, got)
			}
		})
	}
}

func testDBExecutiveSetWriterCookie(t *testing.T, dbType string) {
	suite := []struct {
		desc         string
		writerName   string
		writerSecret string
		cookie       []byte
		expectErr    string
		expectCookie []byte
	}{
		{
			desc:       "Empty writer returns error",
			writerName: "writer-doesnt-exist",
			expectErr:  "Writer not found",
		},
		{
			desc:         "Bad writer secret returns error",
			writerName:   "writer1",
			writerSecret: "invalid",
			expectErr:    "Writer not found",
		},
		{
			desc:         "Existing writer",
			writerName:   "writer1",
			cookie:       []byte{1},
			expectCookie: []byte{1},
		},
	}

	for _, testCase := range suite {
		t.Run(testCase.desc, func(t *testing.T) {
			u := newDbExecTestUtil(t, dbType)
			defer u.Close()

			gotErr := u.e.SetWriterCookie(testCase.writerName, testCase.writerSecret, testCase.cookie)

			if want, got := testCase.expectErr, gotErr; (got == nil && want != "") || (got != nil && want != got.Error()) {
				t.Errorf("Expected: %v, got %v", want, got)
			}

			if testCase.expectCookie != nil {
				gotCookie, err := u.e.GetWriterCookie(testCase.writerName, testCase.writerSecret)
				if err != nil {
					t.Fatalf("Unexpected error: %+v", err)
				}
				if diff := cmp.Diff(testCase.expectCookie, gotCookie); diff != "" {
					t.Errorf("Cookie differs:\n+%v", diff)
				}
			}
		})
	}
}

func testDBExecutiveReadRow(t *testing.T, dbType string) {
	suite := []struct {
		desc       string
		familyName string
		tableName  string
		where      map[string]interface{}
		expectOut  map[string]interface{}
		expectErr  string
	}{
		{
			desc:       "Table not found",
			familyName: "nonExistantFamily",
			tableName:  "nonExistantTable",
			where:      nil,
			expectOut:  nil,
			expectErr:  "Table not found",
		},
		{
			desc:       "Row not found",
			familyName: "family1",
			tableName:  "table10",
			where:      map[string]interface{}{"field1": 1234},
			expectOut:  map[string]interface{}{},
			expectErr:  "",
		},
		{
			desc:       "Row found",
			familyName: "family1",
			tableName:  "table10",
			where:      map[string]interface{}{"field1": 1},
			expectOut: map[string]interface{}{
				"field1": int64(1),
				"field2": "foo",
				"field3": float64(1.2),
			},
			expectErr: "",
		},
	}

	for _, testCase := range suite {
		t.Run(testCase.desc, func(t *testing.T) {
			u := newDbExecTestUtil(t, dbType)
			defer u.Close()

			gotOut, gotErr := u.e.ReadRow(testCase.familyName, testCase.tableName, testCase.where)

			if diff := cmp.Diff(testCase.expectOut, gotOut); diff != "" {
				t.Errorf("Out differs\n%s", diff)
			}
			if want, got := testCase.expectErr, gotErr; (got == nil && want != "") || (got != nil && want != got.Error()) {
				t.Errorf("Expected: %v, got %v", want, got)
			}
		})
	}
}

func testDBExecutiveDropTable(t *testing.T, dbType string) {
	u := newDbExecTestUtil(t, dbType)
	defer u.Close()

	err := u.e.CreateTable("family1",
		"delete_test",
		[]string{"field1"},
		[]schema.FieldType{schema.FTString},
		[]string{"field1"},
	)
	require.NoError(t, err)

	// verify the table exists and has no rows
	row := u.db.QueryRow("SELECT COUNT(*) FROM family1___delete_test")
	var cnt sql.NullInt64
	err = row.Scan(&cnt)
	require.NoError(t, err)
	require.EqualValues(t, 0, cnt.Int64)

	err = u.e.DropTable(schema.FamilyTable{Family: "family1", Table: "delete_test"})
	require.NoError(t, err)

	// assert that we can't query the table anymore
	row = u.db.QueryRow("SELECT COUNT(*) FROM family1___delete_test")
	err = row.Scan(&cnt)
	switch dbType {
	case "sqlite3":
		require.EqualError(t, err, "no such table: family1___delete_test")
	case "mysql":
		require.EqualError(t, err, "Error 1146: Table 'ctldb.family1___delete_test' doesn't exist")
	default:
		require.Fail(t, "unknown db type: "+dbType)
	}

	// double check the dml
	row = u.db.QueryRow("select statement from ctlstore_dml_ledger order by seq desc limit 1")
	var statement string
	err = row.Scan(&statement)
	require.NoError(t, err)
	require.EqualValues(t, "DROP TABLE IF EXISTS family1___delete_test", statement)
}

func testDBExecutiveClearTable(t *testing.T, dbType string) {
	u := newDbExecTestUtil(t, dbType)
	defer u.Close()

	err := u.e.CreateTable("family1",
		"table5",
		[]string{"field1", "field2"},
		[]schema.FieldType{schema.FTString, schema.FTInteger},
		[]string{"field1", "field2"},
	)
	if err != nil {
		t.Fatalf("Unexpected error calling CreateTable: %+v", err)
	}

	_, err = u.db.Exec(`INSERT into family1___table5
		(field1,field2)
		VALUES	('1',2) `)
	if err != nil {
		t.Fatal(err)
	}

	err = u.e.ClearTable(schema.FamilyTable{Family: "family1", Table: "table5"})
	if err != nil {
		t.Fatalf("Unexpected error calling ClearTable: %+v", err)
	}

	row := u.db.QueryRow("SELECT COUNT(*) FROM family1___table5")
	var cnt sql.NullInt64
	err = row.Scan(&cnt)
	if err != nil {
		t.Fatalf("Unexpected error scanning result: %+v", err)
	}

	if want, got := 0, cnt; !got.Valid || int(got.Int64) != want {
		t.Errorf("Expected %+v, got %+v", want, got)
	}
}

func testDBExecutiveReadFamilyTableNames(t *testing.T, dbType string) {
	if dbType != "mysql" {
		t.Skip("skipping test when db is not mysql")
	}

	u := newDbExecTestUtil(t, dbType)
	defer u.Close()

	tables, err := u.e.ReadFamilyTableNames(schema.FamilyName{Name: "family1"})
	if err != nil {
		t.Fatalf("Unexpected error calling Reading Family Table Names: %+v", err)
	}

	if want, got := 5, len(tables); got != want {
		t.Errorf("Expected %+v tables, got %+v", want, got)
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Table < tables[j].Table
	})

	expected := []schema.FamilyTable{
		{
			Family: "family1",
			Table:  "binary_table1",
		},
		{
			Family: "family1",
			Table:  "table1",
		},
		{
			Family: "family1",
			Table:  "table10",
		},
		{
			Family: "family1",
			Table:  "table100",
		},
		{
			Family: "family1",
			Table:  "table11",
		},
	}

	for i, table := range tables {
		if table.Family != "family1" {
			t.Errorf("Expected family1, got %+v", table.Family)
		}
		if table.Table != expected[i].Table {
			t.Errorf("Invalid table name. Expected %s, got %s,", expected[i].Table, table.Table)
		}
	}
}
