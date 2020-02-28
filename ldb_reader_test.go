package ctlstore

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/ldbwriter"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/utils"
	"github.com/stretchr/testify/require"
)

type testKVStruct struct {
	Key string `ctlstore:"key"`
	Val string `ctlstore:"value"`
}

func benchmarkGetRowByKey(b *testing.B, target interface{}) {
	ctx := context.Background()
	db, teardown := ldb.LDBForTest(b)
	defer teardown()

	_, err := db.Exec(initSQLForReadKeyByRow)
	if err != nil {
		b.Errorf("Unexpected error encountered when bootstrapping test database: %v", err)
	}
	reader := LDBReader{Db: db}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		found, err := reader.GetRowByKey(
			ctx,
			target,
			"foo",
			"bar",
			utils.InterfaceSlice([]string{"foo"})...,
		)
		if !found {
			b.Fatal("should have been found")
		}
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStructGetRowByKey(b *testing.B) {
	benchmarkGetRowByKey(b, &testKVStruct{})
}

func BenchmarkMapGetRowByKey(b *testing.B) {
	benchmarkGetRowByKey(b, map[string]interface{}{})
}

func TestGetLedgerLatency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, teardown := ldb.LDBForTest(t)
	defer teardown()

	reader := &LDBReader{Db: db}

	latency, err := reader.GetLedgerLatency(ctx)
	require.Equal(t, ErrNoLedgerUpdates, err)

	writer := ldbwriter.SqlLdbWriter{Db: db}
	err = writer.ApplyDMLStatement(ctx, schema.NewTestDMLStatement("create table foo (var VARCHAR)"))
	require.NoError(t, err)

	require.NoError(t, err)
	latency, err = reader.GetLedgerLatency(ctx)
	require.NoError(t, err)
	require.True(t, latency < time.Second, "weird latency: %v", latency)
}

func TestLDBMustExistForReader(t *testing.T) {
	oldGlobal := globalLDBDirPath
	defer func() { globalLDBDirPath = oldGlobal }()

	var err error
	globalLDBDirPath, err = ioutil.TempDir("", "ldb_must_exist")
	require.NoError(t, err)

	r, err := Reader()
	require.EqualError(t, err, fmt.Sprintf("no LDB found at %s/%s", globalLDBDirPath, ldb.DefaultLDBFilename))
	require.Nil(t, r)

	r, err = ReaderForPath("/does/not/exist/ldb.db")
	require.EqualError(t, err, "no LDB found at /does/not/exist/ldb.db")
	require.Nil(t, r)
}

func TestGetRowsByKeyPrefix(t *testing.T) {
	type mrStruct struct {
		K1  string `ctlstore:"k1"`
		K2  string `ctlstore:"k2"`
		Val int64  `ctlstore:"val"`
	}
	for _, test := range []struct {
		desc       string
		family     string
		table      string
		key        interface{}
		targetFunc func() interface{}
		err        error
		expected   []interface{}
	}{
		{
			desc:       "single key [map]",
			family:     "foo",
			table:      "multirow",
			key:        []interface{}{"a"},
			targetFunc: func() interface{} { return map[string]interface{}{} },
			expected: []interface{}{
				map[string]interface{}{"k1": "a", "k2": "A", "val": int64(42)},
				map[string]interface{}{"k1": "a", "k2": "B", "val": int64(43)},
			},
			err: nil,
		},
		{
			desc:       "single key [struct]",
			family:     "foo",
			table:      "multirow",
			key:        []interface{}{"a"},
			targetFunc: func() interface{} { return &mrStruct{} },
			expected: []interface{}{
				&mrStruct{K1: "a", K2: "A", Val: 42},
				&mrStruct{K1: "a", K2: "B", Val: 43},
			},
			err: nil,
		},
		{
			desc:       "all keys [map]",
			family:     "foo",
			table:      "multirow",
			key:        []interface{}{"a", "A"},
			targetFunc: func() interface{} { return map[string]interface{}{} },
			expected: []interface{}{
				map[string]interface{}{"k1": "a", "k2": "A", "val": int64(42)},
			},
			err: nil,
		},
		{
			desc:       "all keys [struct]",
			family:     "foo",
			table:      "multirow",
			key:        []interface{}{"a", "A"},
			targetFunc: func() interface{} { return &mrStruct{} },
			expected: []interface{}{
				&mrStruct{K1: "a", K2: "A", Val: 42},
			},
			err: nil,
		},
		{
			desc:       "no keys [map]",
			family:     "foo",
			table:      "multirow",
			key:        []interface{}{},
			targetFunc: func() interface{} { return map[string]interface{}{} },
			expected: []interface{}{
				map[string]interface{}{"k1": "a", "k2": "A", "val": int64(42)},
				map[string]interface{}{"k1": "a", "k2": "B", "val": int64(43)},
				map[string]interface{}{"k1": "b", "k2": "B", "val": int64(44)},
			},
			err: nil,
		},
		{
			desc:       "no keys [struct]",
			family:     "foo",
			table:      "multirow",
			key:        []interface{}{},
			targetFunc: func() interface{} { return &mrStruct{} },
			expected: []interface{}{
				&mrStruct{K1: "a", K2: "A", Val: 42},
				&mrStruct{K1: "a", K2: "B", Val: 43},
				&mrStruct{K1: "b", K2: "B", Val: 44},
			},
			err: nil,
		},
		{
			desc:       "too many keys supplied",
			family:     "foo",
			table:      "multirow",
			key:        []interface{}{"a", "b", "c", "d"},
			targetFunc: func() interface{} { return map[string]interface{}{} },
			expected:   nil,
			err:        errors.New("too many keys supplied for table's primary key"),
		},
		{
			desc:       "no rows found",
			family:     "foo",
			table:      "multirow",
			key:        []interface{}{"lol nothing here"},
			targetFunc: func() interface{} { return map[string]interface{}{} },
			expected:   nil,
			err:        nil,
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			ctx := context.Background()
			db, teardown := ldb.LDBForTest(t)
			defer teardown()

			_, err := db.Exec(initSQLForReadKeyByRow)
			if err != nil {
				t.Errorf("Unexpected error encountered when bootstrapping test database: %v", err)
			}
			reader := LDBReader{Db: db}
			rows, err := reader.GetRowsByKeyPrefix(
				ctx,
				test.family,
				test.table,
				utils.InterfaceSlice(test.key)...,
			)
			switch {
			case err != nil && test.err == nil:
				t.Fatal(err)
			case err == nil && test.err != nil:
				t.Fatal("Expected err: " + test.err.Error())
			case err != nil:
				require.EqualValues(t, err.Error(), test.err.Error())
			}
			if err != nil {
				// don't bother with the rest of the test if we hit an error
				return
			}
			defer func() {
				err = rows.Close()
				require.NoError(t, err)
			}()
			var res []interface{}
			for rows.Next() {
				target := test.targetFunc()
				err := rows.Scan(target)
				require.NoError(t, err)
				res = append(res, target)
			}
			require.EqualValues(t, test.expected, res)
		})
	}

}

func TestGetRowByKey(t *testing.T) {
	suite := []struct {
		desc        string
		familyName  string
		tableName   string
		key         interface{}
		gotOut      interface{}
		expectOut   interface{}
		expectFound bool
		expectErr   error
	}{
		{
			desc:        "map single-key found",
			familyName:  "foo",
			tableName:   "bar",
			key:         []string{"foo"},
			gotOut:      map[string]interface{}{},
			expectOut:   map[string]interface{}{"key": "foo", "value": "bar"},
			expectFound: true,
			expectErr:   nil,
		},
		{
			desc:        "struct single-key found",
			familyName:  "foo",
			tableName:   "bar",
			key:         []string{"foo"},
			gotOut:      &testKVStruct{},
			expectOut:   &testKVStruct{"foo", "bar"},
			expectFound: true,
			expectErr:   nil,
		},
		{
			desc:        "map single-key not found",
			familyName:  "foo",
			tableName:   "bar",
			key:         []string{"non-existant"},
			gotOut:      map[string]interface{}{},
			expectOut:   map[string]interface{}{},
			expectFound: false,
			expectErr:   nil,
		},
		{
			desc:        "struct single-key not found",
			familyName:  "foo",
			tableName:   "bar",
			key:         []string{"non-existant"},
			gotOut:      &testKVStruct{},
			expectOut:   &testKVStruct{},
			expectFound: false,
			expectErr:   nil,
		},
		{
			desc:        "map composite key found",
			familyName:  "foo",
			tableName:   "composite",
			key:         []string{"foo", "bar"},
			gotOut:      map[string]interface{}{},
			expectOut:   map[string]interface{}{"key": "foo", "value": "bar"},
			expectFound: true,
			expectErr:   nil,
		},
		{
			desc:        "struct composite key found",
			familyName:  "foo",
			tableName:   "composite",
			key:         []string{"foo", "bar"},
			gotOut:      &testKVStruct{},
			expectOut:   &testKVStruct{"foo", "bar"},
			expectFound: true,
			expectErr:   nil,
		},
		{
			desc:        "struct varbinary key query by binary",
			familyName:  "foo",
			tableName:   "varbinarykey",
			key:         []interface{}{[]byte("beef")}, // use []byte key
			gotOut:      &testKVStruct{},
			expectOut:   &testKVStruct{"beef", "moo"},
			expectFound: true,
			expectErr:   nil,
		},
		{
			desc:        "map varbinary key query by binary",
			familyName:  "foo",
			tableName:   "varbinarykey",
			key:         []interface{}{[]byte("beef")}, // use []byte key
			gotOut:      map[string]interface{}{},
			expectOut:   map[string]interface{}{"key": []byte("beef"), "value": "moo"},
			expectFound: true,
			expectErr:   nil,
		},
		{
			desc:        "struct varbinary key query by string",
			familyName:  "foo",
			tableName:   "varbinarykey",
			key:         []interface{}{"beef"}, // use string key
			gotOut:      &testKVStruct{},
			expectOut:   &testKVStruct{"beef", "moo"},
			expectFound: true,
			expectErr:   nil,
		},
		{
			desc:        "map varbinary key query by string",
			familyName:  "foo",
			tableName:   "varbinarykey",
			key:         []interface{}{"beef"}, // use string key
			gotOut:      map[string]interface{}{},
			expectOut:   map[string]interface{}{"key": []byte("beef"), "value": "moo"},
			expectFound: true,
			expectErr:   nil,
		},
	}

	for _, testCase := range suite {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx := context.Background()
			db, teardown := ldb.LDBForTest(t)
			defer teardown()
			_, err := db.Exec(initSQLForReadKeyByRow)
			if err != nil {
				t.Errorf("Unexpected error encountered when bootstrapping test database: %v", err)
			}

			reader := LDBReader{Db: db}
			gotFound, gotErr := reader.GetRowByKey(
				ctx,
				testCase.gotOut,
				testCase.familyName,
				testCase.tableName,
				utils.InterfaceSlice(testCase.key)...,
			)

			require.EqualValues(t, testCase.expectErr, gotErr)
			require.EqualValues(t, testCase.expectFound, gotFound)

			if diff := cmp.Diff(testCase.gotOut, testCase.expectOut); diff != "" {
				t.Errorf("Out mismatch\n%s", diff)
			}
		})
	}
}

func TestLDBReaderEmptyFileHandling(t *testing.T) {
	ctx := context.Background()
	dbPath, teardown := ldb.NewLDBTmpPath(t)
	defer teardown()

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Couldn't open SQLite db, error %v", err)
	}

	reader := LDBReader{Db: db}

	var foo struct{}
	gotFound, gotErr := reader.GetRowByKey(ctx, &foo, "foo", "bar", "foo")

	if want, got := false, gotFound; want != got {
		t.Errorf("Expected %v, got %v", want, got)
	}

	if want, got := "Table not found", gotErr; gotErr == nil || want != got.Error() {
		t.Errorf("Expected %v, got %v", want, got)
	}

	db2, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Couldn't open SQLite db, error %v", err)
	}

	err = ldb.EnsureLdbInitialized(context.Background(), db2)
	if err != nil {
		t.Fatalf("Couldn't initialize SQLite db, error %v", err)
	}

	initSQL := `
CREATE TABLE foo___bar (
	key VARCHAR PRIMARY KEY,
	value VARCHAR
);

INSERT INTO foo___bar VALUES('foo', 'bar');
`
	_, err = db2.Exec(initSQL)
	if err != nil {
		t.Errorf("Unexpected error encountered when bootstrapping test database: %v", err)
	}

	gotFound, gotErr = reader.GetRowByKey(ctx, &foo, "foo", "bar", "foo")

	if want, got := true, gotFound; want != got {
		t.Errorf("Expected %v, got %v", want, got)
	}

	if want, got := "", gotErr; gotErr != nil && want != got.Error() {
		t.Errorf("Expected %v, got %v", want, got)
	}
}

func TestLDBReaderPing(t *testing.T) {
	ctx := context.Background()
	dbPath, teardown := ldb.NewLDBTmpPath(t)
	defer teardown()

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Couldn't open SQLite db, error %v", err)
	}

	reader := LDBReader{Db: db}

	if want, got := false, reader.Ping(ctx); want != got {
		t.Errorf("Expected %v, got %v", want, got)
	}

	err = ldb.EnsureLdbInitialized(context.Background(), db)

	_, err = db.Exec(
		fmt.Sprintf("REPLACE INTO %s (id, seq) VALUES(?, ?)", ldb.LDBSeqTableName),
		ldb.LDBSeqTableID, 1)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}

	if want, got := true, reader.Ping(ctx); want != got {
		t.Errorf("Expected %v, got %v", want, got)
	}
}

func TestLDBVersioning(t *testing.T) {
	require := require.New(t)
	globalLDBReadOnly = false

	// Initialize a new temporary directory, which we'll use
	// as the path for storing timestamp-versioned LDBs.
	path, err := ioutil.TempDir("", "ldb-versioning-tmp-path")
	require.NoError(err)
	defer os.RemoveAll(path)

	// Initialize an LDBVersioned reader pointing at this directory
	// which currently does not contain any LDBs. It should error.
	reader, err := newLDBReaderFromPath(path, true)
	require.Error(err, "expected new reader to fail when directory contains no LDBs")
	require.Nil(reader)

	// So now we initialize a new LDB in this path, which should
	// allow us to create a new LDB without error.
	generateVersionedLDB(t, path, int64(1500000000000))

	// Create a new reader, which should not fail this time, since an LDB exists
	// in the associated folder.
	reader, err = newLDBReaderFromPath(path, true)
	require.NoError(err)
	defer reader.Close()
	require.True(reader.Ping(context.Background()))

	// Verify that this reader is consuming from the LDB we created above, based on
	// what timestamp was written into the LDB on creation.
	require.Equal(int64(1500000000000), getLDBTimestamp(t, reader))

	// Now, create a newer LDB with a higher timestamp. The reader should pick this
	// up and return the new timestamp.
	generateVersionedLDB(t, path, int64(1500000000001))
	time.Sleep(2 * time.Second) // Wait for the reader to pick up the new LDB.
	require.Equal(int64(1500000000001), getLDBTimestamp(t, reader))

	// As a safeguard, should a new LDB be generated with a _lower_ timestamp
	// make sure that the reader does not consume it.
	generateVersionedLDB(t, path, int64(1400000000000))
	time.Sleep(2 * time.Second) // Wait for the reader to pick up the new LDB.
	require.Equal(int64(1500000000001), getLDBTimestamp(t, reader))
}

func generateVersionedLDB(t *testing.T, path string, timestamp int64) {
	require := require.New(t)

	dirPath := filepath.Join(path, fmt.Sprintf("%013d", timestamp))
	require.NoError(os.Mkdir(dirPath, 0755))

	dbPath := filepath.Join(dirPath, "ldb.db")
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(err)

	// Initialize the ctlstore LDB tables:
	require.NoError(ldb.EnsureLdbInitialized(context.Background(), db))
	_, err = db.Exec(
		fmt.Sprintf("REPLACE INTO %s (id, seq) VALUES(?, ?)", ldb.LDBSeqTableName),
		ldb.LDBSeqTableID, 1)
	require.NoError(err)

	// Initialize a fake ctlstore table which will store the timestamp
	// of this LDB.
	_, err = db.Exec(`
		CREATE TABLE versioning___curr_ts (
			key VARCHAR PRIMARY KEY,
			timestamp BIGINT
		);
		
		INSERT INTO versioning___curr_ts VALUES('now', ?);
	`, timestamp)
	require.NoError(err)
}

func getLDBTimestamp(t *testing.T, reader *LDBReader) int64 {
	require := require.New(t)

	var out struct {
		timestamp int64 `ctlstore:"timestamp"`
	}
	ok, err := reader.GetRowByKey(context.Background(), &out, "versioning", "curr_ts", "now")
	require.True(ok)
	require.NoError(err)
	return out.timestamp
}

// sql that is used to initialize the tests and benchmarks that
// exercise ReadKeyByRow()
const initSQLForReadKeyByRow = ` 
 		CREATE TABLE foo___bar (
			key VARCHAR PRIMARY KEY,
			value VARCHAR
		);
		INSERT INTO foo___bar VALUES('foo', 'bar');

		CREATE TABLE foo___composite (
			key VARCHAR,
			value VARCHAR,
			PRIMARY KEY(key, value)
		);
		INSERT INTO foo___composite VALUES('foo', 'bar');

		CREATE TABLE foo___varbinarykey (
			key  VARBINARY,
			value VARCHAR,
 			PRIMARY KEY(key)
		);
		INSERT INTO foo___varbinarykey (key, value) VALUES (x'62656566', "moo"); /* key is "beef" */
		
		CREATE TABLE foo___multirow (
		  k1 varchar,
		  k2 varchar,
		  val int ,
		  primary key (k1, k2)
		);
		INSERT INTO foo___multirow (k1,k2,val) VALUES ('a', 'A', 42);
		INSERT INTO foo___multirow (k1,k2,val) VALUES ('a', 'B', 43);
		INSERT INTO foo___multirow (k1,k2,val) VALUES ('b', 'B', 44);
`
