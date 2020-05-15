package sqlgen

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/ctlstore/pkg/ctldb"
	"github.com/segmentio/ctlstore/pkg/schema"
	_ "github.com/segmentio/go-sqlite3"
)

func TestMetaTableAsCreateTableDDL(t *testing.T) {
	famName, _ := schema.NewFamilyName("family1")
	tblName, _ := schema.NewTableName("table1")
	tbl := &MetaTable{
		DriverName: "sqlite3",
		FamilyName: famName,
		TableName:  tblName,
		Fields: []schema.NamedFieldType{
			{schema.FieldName{Name: "field1"}, schema.FTString},
			{schema.FieldName{Name: "field2"}, schema.FTInteger},
			{schema.FieldName{Name: "field3"}, schema.FTDecimal},
		},
		KeyFields: schema.PrimaryKey{Fields: []schema.FieldName{{Name: "field1"}}},
	}

	got, err := tbl.AsCreateTableDDL()
	if err != nil {
		t.Fatalf("Unexpected error %v", err)
	}

	want := `CREATE TABLE family1___table1 (` +
		`"field1" VARCHAR(191), ` +
		`"field2" INTEGER, ` +
		`"field3" REAL, ` +
		`PRIMARY KEY("field1")` +
		`);`

	if want != got {
		t.Errorf("Expected '%v', got '%v'", want, got)
	}

	ddl := want
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Unexpected error opening SQLite3 DB: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(want)
	if err != nil {
		t.Errorf("Error executing generated SQL statement: %v", err)
	}

	rows, err := db.Query("SELECT name, type, pk FROM pragma_table_info(?)", "family1___table1")
	if err != nil {
		t.Fatalf("Unexpected error executing statement: %v", err)
	}
	defer rows.Close()

	foundColCount := 0
	for rows.Next() {
		var name, colType string
		var pk int
		err := rows.Scan(&name, &colType, &pk)
		if err != nil {
			t.Fatalf("Unexpected error scanning row: %v", err)
		}

		if name == "field1" && colType == "VARCHAR(191)" && pk == 1 {
			foundColCount++
		}
		if name == "field2" && colType == "INTEGER" && pk == 0 {
			foundColCount++
		}
		if name == "field3" && colType == "REAL" && pk == 0 {
			foundColCount++
		}
	}

	if want, got := 3, foundColCount; want != got {
		t.Logf("DDL: %v", ddl)
		t.Errorf("Expected to find %d columns, but found %d", want, got)
	}
}

func TestMetaTableAddColumnDDL(t *testing.T) {
	famName, _ := schema.NewFamilyName("family1")
	tblName, _ := schema.NewTableName("table1")
	tbl := &MetaTable{
		DriverName: "sqlite3",
		FamilyName: famName,
		TableName:  tblName,
		Fields: []schema.NamedFieldType{
			{Name: schema.FieldName{Name: "field1"}, FieldType: schema.FTString},
		},
		KeyFields: schema.PrimaryKey{Fields: []schema.FieldName{{Name: "field1"}}},
	}

	ddl, err := tbl.AddColumnDDL(schema.FieldName{Name: "field2"}, schema.FTInteger)
	if err != nil {
		t.Errorf("Unexpected error calling AddColumnDDL method: %v", err)
	}

	{
		want := `ALTER TABLE family1___table1 ADD COLUMN "field2" INTEGER`
		got := ddl
		if want != got {
			t.Errorf("Expected SQL to be '%s', got: '%s'", want, got)
		}
	}

	{
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Unexpected error opening SQLite3 DB: %v", err)
		}
		defer db.Close()

		ddl = `CREATE TABLE family1___table1 ("field1" VARCHAR PRIMARY KEY); ` + ddl
		_, err = db.Exec(ddl)
		if err != nil {
			t.Errorf("Error executing generated SQL statement: %v", err)
		}

		rows, err := db.Query("SELECT name, type, pk FROM pragma_table_info(?)", "family1___table1")
		if err != nil {
			t.Fatalf("Unexpected error executing statement: %v", err)
		}
		defer rows.Close()

		foundColCount := 0
		for rows.Next() {
			var name, colType string
			var pk int
			err := rows.Scan(&name, &colType, &pk)
			if err != nil {
				t.Fatalf("Unexpected error scanning row: %v", err)
			}

			if name == "field1" && colType == "VARCHAR" && pk == 1 {
				foundColCount++
			}
			if name == "field2" && colType == "INTEGER" && pk == 0 {
				foundColCount++
			}
		}

		if want, got := 2, foundColCount; want != got {
			t.Errorf("Expected to find %d columns, but found %d", want, got)
		}
	}
}

func TestMetaTableUpsertDML(t *testing.T) {
	for _, test := range []struct {
		name string
		meta func() MetaTable
		row  func() []interface{}
		want string
	}{
		{
			name: "basic test",
			meta: func() MetaTable {
				famName, _ := schema.NewFamilyName("family1")
				tblName, _ := schema.NewTableName("table1")
				return MetaTable{
					FamilyName: famName,
					TableName:  tblName,
					Fields: []schema.NamedFieldType{
						{schema.FieldName{Name: "field1"}, schema.FTString},
						{schema.FieldName{Name: "field2"}, schema.FTString},
						{schema.FieldName{Name: "field3"}, schema.FTInteger},
						{schema.FieldName{Name: "field4"}, schema.FTByteString},
					},
					KeyFields: schema.PrimaryKey{Fields: []schema.FieldName{{Name: "field1"}, {Name: "field2"}}},
				}
			},
			row: func() []interface{} {
				encoded := base64.StdEncoding.EncodeToString([]byte{1, 2, 3})
				return []interface{}{"hello", "there", 123, encoded}
			},
			want: `REPLACE INTO family1___table1 ("field1","field2","field3","field4") ` +
				`VALUES('hello','there',123,x'010203')`,
		},
		{
			name: "upsert with null in key column",
			meta: func() MetaTable {
				famName, _ := schema.NewFamilyName("family1")
				tblName, _ := schema.NewTableName("table1")
				return MetaTable{
					FamilyName: famName,
					TableName:  tblName,
					Fields: []schema.NamedFieldType{
						{schema.FieldName{Name: "field1"}, schema.FTString},
						{schema.FieldName{Name: "field2"}, schema.FTString},
						{schema.FieldName{Name: "field3"}, schema.FTInteger},
					},
					KeyFields: schema.PrimaryKey{Fields: []schema.FieldName{{Name: "field1"}, {Name: "field2"}}},
				}
			},
			row: func() []interface{} {
				return []interface{}{"a\x00b", "there", 123}
			},
			want: `REPLACE INTO family1___table1 ("field1","field2","field3") ` +
				`VALUES(x'610062','there',123)`,
		},
		{
			name: "upsert with null in non-key column",
			meta: func() MetaTable {
				famName, _ := schema.NewFamilyName("family1")
				tblName, _ := schema.NewTableName("table1")
				return MetaTable{
					FamilyName: famName,
					TableName:  tblName,
					Fields: []schema.NamedFieldType{
						{schema.FieldName{Name: "field1"}, schema.FTString},
						{schema.FieldName{Name: "field2"}, schema.FTString},
						{schema.FieldName{Name: "field3"}, schema.FTInteger},
					},
					KeyFields: schema.PrimaryKey{Fields: []schema.FieldName{{Name: "field1"}, {Name: "field2"}}},
				}
			},
			row: func() []interface{} {
				return []interface{}{"hi", "a\x00b", 123}
			},
			want: `REPLACE INTO family1___table1 ("field1","field2","field3") ` +
				`VALUES('hi',x'610062',123)`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tbl := test.meta()
			got, err := tbl.UpsertDML(test.row())
			require.NoError(t, err)
			require.EqualValues(t, test.want, got)
		})
	}
}

func TestMetaTableDeleteDML(t *testing.T) {
	for _, test := range []struct {
		name string
		meta func() MetaTable
		row  func() []interface{}
		want string
	}{
		{
			name: "basic test",
			meta: func() MetaTable {
				famName, _ := schema.NewFamilyName("family1")
				tblName, _ := schema.NewTableName("table1")
				return MetaTable{
					FamilyName: famName,
					TableName:  tblName,
					Fields: []schema.NamedFieldType{
						{schema.FieldName{Name: "field1"}, schema.FTString},
						{schema.FieldName{Name: "field2"}, schema.FTByteString},
						{schema.FieldName{Name: "field3"}, schema.FTInteger},
					},
					KeyFields: schema.PrimaryKey{Fields: []schema.FieldName{{Name: "field1"}, {Name: "field2"}}},
				}
			},
			row: func() []interface{} {
				encoded := base64.StdEncoding.EncodeToString([]byte{1, 2, 3})
				return []interface{}{"hello", encoded}
			},
			want: `DELETE FROM family1___table1 WHERE ` +
				`"field1" = 'hello' AND ` +
				`"field2" = x'010203'`,
		},
		{
			name: "delete with null in key column",
			meta: func() MetaTable {
				famName, _ := schema.NewFamilyName("family1")
				tblName, _ := schema.NewTableName("table1")
				return MetaTable{
					FamilyName: famName,
					TableName:  tblName,
					Fields: []schema.NamedFieldType{
						{schema.FieldName{Name: "field1"}, schema.FTString},
						{schema.FieldName{Name: "field2"}, schema.FTString},
					},
					KeyFields: schema.PrimaryKey{Fields: []schema.FieldName{{Name: "field1"}}},
				}
			},
			row: func() []interface{} {
				return []interface{}{"a\x00b"}
			},
			want: `DELETE FROM family1___table1 WHERE "field1" = x'610062'`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tbl := test.meta()
			got, err := tbl.DeleteDML(test.row())
			require.NoError(t, err)
			require.EqualValues(t, test.want, got)
		})
	}
}

func TestMetaTableClearTableDDL(t *testing.T) {
	famName, _ := schema.NewFamilyName("family1")
	tblName, _ := schema.NewTableName("table1")
	tbl := MetaTable{
		FamilyName: famName,
		TableName:  tblName,
		Fields: []schema.NamedFieldType{
			{schema.FieldName{Name: "field1"}, schema.FTString},
			{schema.FieldName{Name: "field2"}, schema.FTByteString},
		},
		KeyFields: schema.PrimaryKey{Fields: []schema.FieldName{{Name: "field1"}}},
	}

	got := tbl.ClearTableDDL()
	want := `DELETE FROM family1___table1`

	if got != want {
		t.Errorf("Expected: %v, Got: %v", want, got)
	}
}

func TestMetaTableDropTableDDL(t *testing.T) {
	famName, _ := schema.NewFamilyName("family1")
	tblName, _ := schema.NewTableName("table1")
	tbl := MetaTable{
		FamilyName: famName,
		TableName:  tblName,
		Fields: []schema.NamedFieldType{
			{schema.FieldName{Name: "field1"}, schema.FTString},
		},
		KeyFields: schema.PrimaryKey{Fields: []schema.FieldName{{Name: "field1"}}},
	}

	got := tbl.DropTableDDL()
	require.EqualValues(t, `DROP TABLE IF EXISTS family1___table1`, got)
}

func TestSQLQuote(t *testing.T) {
	suite := []struct {
		desc   string
		input  interface{}
		output string
	}{
		{"int", int(1), "1"},
		{"uint", uint(1), "1"},
		{"int8", int8(1), "1"},
		{"uint8", uint8(1), "1"},
		{"int32", int32(1), "1"},
		{"uint32", uint32(1), "1"},
		{"int64", int64(1), "1"},
		{"uint64", uint64(1), "1"},
		{"float32", float32(1.01), "1.01"},
		{"float64", float64(1.01), "1.01"},
		{"bool", false, "false"},
		{"string", "hello rick's µ", "'hello rick''s µ'"},
		{"[]byte", []byte{0xDE, 0xAD, 0xBE, 0xEF}, "x'deadbeef'"},
		{"Stringer", reflect.TypeOf(12345), "int"},
	}

	for caseIdx, _testCase := range suite {
		testCase := _testCase // closure capture screws us
		testName := fmt.Sprintf("%d_%s", caseIdx, testCase.desc)

		t.Run(testName, func(t *testing.T) {
			sqliteDb, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			mysqlDb, err := sql.Open("mysql", ctldb.GetTestCtlDBDSN(t))
			if err != nil {
				t.Fatalf("Can't connect to local MySQL: %v", err)
			}

			dbs := []*sql.DB{sqliteDb, mysqlDb}
			for _, db := range dbs {
				dbName := SqlDriverToDriverName(db.Driver())
				t.Run(testName+"_"+dbName, func(t *testing.T) {
					ctx := context.Background()
					quoted, err := SQLQuote(testCase.input)
					_ = ctx
					if err != nil {
						t.Errorf("Unexpected error: %v", err)
					} else if want, got := testCase.output, quoted; want != got {
						t.Errorf("Expected %v, got %v", want, got)
					}
				})
			}
		})
	}
}

func TestFieldTypeSQLResolution(t *testing.T) {
	stripColTypeForDB := func(dbType string, ct string) string {
		if dbType == "mysql" {
			parIdx := strings.Index(ct, "(")
			if parIdx == -1 {
				return ct
			}
			return ct[:parIdx]
		} else if dbType == "sqlite3" {
			return ct
		} else {
			return fmt.Sprintf("ERROR[UNKNOWN DB TYPE='%s']", dbType)
		}
	}

	for wantFt, dbToColType := range fieldTypeToSQLMap {
		for dbType, sqlType := range dbToColType {
			sqlColType := stripColTypeForDB(dbType, sqlType)
			gotFt, ok := schema.SqlTypeToFieldType(sqlColType)
			if !ok {
				t.Errorf("SQL type %v (aka %v) not resolvable to a field", sqlType, sqlColType)
			} else if wantFt != gotFt {
				t.Errorf("Expected %v(%v) to resolve to %v, but got %v", sqlType, sqlColType, wantFt, gotFt)
			}
		}
	}
}
