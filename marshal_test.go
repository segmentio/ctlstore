package ctlstore

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/scanfunc"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/utils"
	"github.com/stretchr/testify/assert"
)

type testNUMTSBasic struct {
	FooWhat string `ctlstore:"foo"`
	BarWhat string `ctlstore:"bar"`
}

type testNUMTSNoTags struct {
	FooWhat string
	BarWhat string
}

// TestScanFuncMap exercises the scanFuncMap function, and
// specifically, the placeholder functionality.  A table will be
// created that has a column for every supported sqlite3 type.
// This test will read a row and verify that the deserialized
// values make sense.
func TestScanFuncMap(t *testing.T) {
	initSQL := `
		CREATE TABLE test___scanmap (
			key VARCHAR PRIMARY KEY,
			val_varchar VARCHAR,
			val_text TEXT,
			val_clob CLOB,
			val_int INT,
			val_tinyint TINYINT,
			val_bigint BIGINT,
			val_float FLOAT,
			val_double DOUBLE,
			val_boolean BOOLEAN,
			val_date DATE,
			val_datetime DATETIME,
			val_blob BLOB
		);
		INSERT INTO test___scanmap VALUES('foo', 
			'varchar value',
			'text value',
			'clob value',
			1,
			2,
			3,
			3.14,
			3.1415,
			true,
			'1976-01-21',
			'1976-01-21 01:02:03',
			x'beef'
		);
    `
	ctx := context.Background()
	db, teardown := ldb.LDBForTest(t)
	defer teardown()

	if _, err := db.Exec(initSQL); err != nil {
		t.Fatal(err)
	}
	out := make(map[string]interface{})
	reader := NewLDBReaderFromDB(db)
	found, err := reader.GetRowByKey(
		ctx,
		out,
		"test",
		"scanmap",
		utils.InterfaceSlice([]string{"foo"})...,
	)
	assert.True(t, found)
	assert.NoError(t, err)
	assert.Equal(t, "varchar value", out["val_varchar"])
	assert.Equal(t, "text value", out["val_text"])
	assert.Equal(t, "clob value", out["val_clob"])
	assert.Equal(t, int64(1), out["val_int"])
	assert.Equal(t, int64(2), out["val_tinyint"])
	assert.Equal(t, int64(3), out["val_bigint"])
	assert.Equal(t, float64(3.14), out["val_float"])
	assert.Equal(t, float64(3.1415), out["val_double"])
	assert.Equal(t, true, out["val_boolean"])
	assert.Equal(t, time.Date(1976, time.January, 21, 0, 0, 0, 0, time.UTC), out["val_date"])
	assert.Equal(t, time.Date(1976, time.January, 21, 1, 2, 3, 0, time.UTC), out["val_datetime"])
	assert.Equal(t, []byte{'\xbe', '\xef'}, out["val_blob"])
}

func TestNewUnmarshalTargetSlice(t *testing.T) {
	suite := []struct {
		desc        string
		initial     interface{}
		valuesToSet map[string]interface{}
		expected    interface{}
		expectError interface{}
	}{
		{
			desc:    "Basic slice setup",
			initial: &testNUMTSBasic{FooWhat: "not-foo", BarWhat: "not-bar"},
			valuesToSet: map[string]interface{}{
				"foo": "foo",
				"bar": "bar",
			},
			expected: &testNUMTSBasic{FooWhat: "foo", BarWhat: "bar"},
		},
		{
			desc:        "Checks target param is pointer",
			initial:     "not-a-ptr",
			expectError: scanfunc.ErrUnmarshalUnsupportedType,
		},
		{
			desc:        "Checks target param points to a struct",
			initial:     &[]byte{},
			expectError: scanfunc.ErrUnmarshalUnsupportedType,
		},
		{
			desc:    "Columns with no matching tagged fields are no-oped",
			initial: &testNUMTSNoTags{FooWhat: "not-foo", BarWhat: "not-bar"},
			valuesToSet: map[string]interface{}{
				"foo": "foo",
				"bar": "bar",
			},
			expected: &testNUMTSNoTags{FooWhat: "not-foo", BarWhat: "not-bar"},
		},
	}

	for _, testCase := range suite {
		t.Run(testCase.desc, func(t *testing.T) {
			target := testCase.initial
			cols := []schema.DBColumnMeta{}
			colIndices := map[string]int{}

			if testCase.valuesToSet != nil {
				i := 0
				for name := range testCase.valuesToSet {
					cols = append(cols, schema.DBColumnMeta{
						Name: name,
					})
					colIndices[name] = i
					i++
				}
			}

			tslice, err := scanfunc.NewUnmarshalTargetSlice(target, cols)
			if err != nil {
				if testCase.expectError != nil {
					if !reflect.DeepEqual(err, testCase.expectError) {
						t.Errorf("Expected error %v, got %v", testCase.expectError, err)
					}
				} else {
					t.Fatalf("Encountered unexpected error building target slice: %v\n", err)
				}
			}

			for columnName, value := range testCase.valuesToSet {
				index, found := colIndices[columnName]
				if !found {
					t.Fatalf("Something very unexpected happened")
				}
				tsliceVal := reflect.ValueOf(tslice)
				// .Elem().Elem() here is because there are two layers of indirection
				// be it the interface{} (a pointer type) which wraps a pointer to
				// receive the value
				elemVal := tsliceVal.Index(index).Elem().Elem()
				elemVal.Set(reflect.ValueOf(value))
			}

			if testCase.expected != nil && !reflect.DeepEqual(target, testCase.expected) {
				t.Errorf("Expected target to be %v, got %v\n", testCase.expected, target)
			}
		})
	}
}
