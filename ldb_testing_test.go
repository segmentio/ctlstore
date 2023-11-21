package ctlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/segmentio/ctlstore/pkg/ldb"
)

func TestLDBTestUtilCreateTableAndInsertRows(t *testing.T) {
	suite := []struct {
		desc       string
		def        LDBTestTableDef
		checkQuery string
		expectRows []map[string]interface{}
	}{
		{
			desc: "Baseline No Rows",
			def: LDBTestTableDef{
				Family: "family1",
				Name:   "table1",
				Fields: [][]string{
					{"string1", "string"},
					{"integer1", "integer"},
				},
				KeyFields: []string{"string1"},
			},
			checkQuery: "SELECT string1, integer1 FROM family1___table1",
		},
		{
			desc: "Insert Rows",
			def: LDBTestTableDef{
				Family: "family1",
				Name:   "table1",
				Fields: [][]string{
					{"string1", "string"},
					{"integer1", "integer"},
				},
				KeyFields: []string{"string1"},
				Rows: [][]interface{}{
					{"hello", 710},
				},
			},
			expectRows: []map[string]interface{}{
				{
					"string1":  "hello",
					"integer1": 710,
				},
			},
		},
	}

	for i, testCase := range suite {
		t.Run(fmt.Sprintf("[%d]%s", i, testCase.desc), func(t *testing.T) {
			db, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				t.Fatalf("Unexpected error: %+v", err)
			}

			err = ldb.EnsureLdbInitialized(context.Background(), db)
			if err != nil {
				t.Fatalf("Couldn't initialize SQLite db, error %v", err)
			}

			tu := &LDBTestUtil{
				DB: db,
				T:  t,
			}
			tu.CreateTable(testCase.def)

			_, err = db.Exec(testCase.checkQuery)
			if err != nil {
				t.Errorf("Query Failed\nQuery: %s\nError: %+v", testCase.checkQuery, err)
			}

			if err == nil && testCase.expectRows != nil {
				actualTable := fmt.Sprintf("%s___%s", testCase.def.Family, testCase.def.Name)
				for _, row := range testCase.expectRows {
					hunks := []string{
						"SELECT COUNT(*) FROM",
						actualTable,
						"WHERE",
					}
					params := []interface{}{}

					clock := 0
					for name, val := range row {
						if clock != 0 {
							hunks = append(hunks, "AND")
						}
						hunks = append(hunks, name, "= ?")
						params = append(params, val)
						clock++
					}

					qs := strings.Join(hunks, " ")
					qrow := db.QueryRow(qs, params...)
					cnt := 0
					err := qrow.Scan(&cnt)

					if err != nil {
						t.Errorf("Table query failed: %+v", err)
					}

					if cnt != 1 {
						t.Errorf("Didn't find row: %+v", row)
					}
				}
			}
		})
	}
}

func TestLDBTestUtilReset(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}
	_, err = db.Exec(strings.Join([]string{
		"CREATE TABLE family1___table1 (field1 VARCHAR);",
		"CREATE TABLE family1___table2 (field1 VARCHAR);",
	}, " "))
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}
	tu := &LDBTestUtil{
		DB: db,
		T:  t,
	}

	tu.Reset()

	for _, table := range []string{"table1", "table2"} {
		_, err = db.Exec("SELECT * FROM family1___" + table)
		if err == nil {
			t.Errorf("Expected to get an error querying family1.%s", table)
		}
	}

}
