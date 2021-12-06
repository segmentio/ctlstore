package sqlite

//func TestRegisterSQLiteWatch(t *testing.T) {
//	dbName := "test_sqlite_watch"
//	var buffer SQLChangeBuffer
//	RegisterSQLiteWatch(dbName, &buffer)
//
//	db, err := sql.Open(dbName, ":memory:")
//	if err != nil {
//		t.Fatalf("Unexpected error: %+v", err)
//	}
//
//	_, err = db.Exec("CREATE TABLE table1 (col1 INTEGER, col2 INTEGER)")
//	if err != nil {
//		t.Fatalf("Unexpected error: %+v", err)
//	}
//
//	_, err = db.Exec("INSERT INTO table1 VALUES(1, 2)")
//	if err != nil {
//		t.Fatalf("Unexpected error: %+v", err)
//	}
//
//	want := SQLiteWatchChange{
//		Op:           sqlite3.SQLITE_INSERT,
//		DatabaseName: "main",
//		TableName:    "table1",
//		OldRowID:     1,
//		NewRowID:     1,
//		OldRow:       nil,
//		NewRow:       []interface{}{int64(1), int64(2)},
//	}
//	got := buffer.Pop()
//	assert.Len(t, got, 1)
//
//	if diff := cmp.Diff(want, got[0]); diff != "" {
//		t.Errorf("Didn't get expected data\n%s", diff)
//	}
//}
//
//func TestSQLiteWatchChangeExtractKeys(t *testing.T) {
//	defaultSetup := `
//		CREATE TABLE table1 (
//			col1 INTEGER,
//			col2 INTEGER,
//			col3 INTEGER,
//			PRIMARY KEY(col1, col2)
//		);
//	`
//	suite := []struct {
//		setup      string
//		desc       string
//		change     SQLiteWatchChange
//		expectKeys [][]interface{}
//	}{
//		{
//			desc:  "Insert",
//			setup: defaultSetup,
//			change: SQLiteWatchChange{
//				Op:           sqlite3.SQLITE_INSERT,
//				DatabaseName: "main",
//				TableName:    "table1",
//				OldRowID:     0,
//				NewRowID:     0,
//				OldRow:       nil,
//				NewRow:       []interface{}{int64(1), int64(2), int64(3)},
//			},
//			expectKeys: [][]interface{}{
//				{
//					pkAndMeta{Name: "col1", Type: "INTEGER", Value: int64(1)},
//					pkAndMeta{Name: "col2", Type: "INTEGER", Value: int64(2)},
//				},
//			},
//		},
//		{
//			desc:  "Update with Same Key",
//			setup: defaultSetup,
//			change: SQLiteWatchChange{
//				Op:           sqlite3.SQLITE_UPDATE,
//				DatabaseName: "main",
//				TableName:    "table1",
//				OldRowID:     0,
//				NewRowID:     0,
//				OldRow:       []interface{}{int64(1), int64(2), int64(3)},
//				NewRow:       []interface{}{int64(1), int64(2), int64(4)},
//			},
//			expectKeys: [][]interface{}{
//				{
//					pkAndMeta{Name: "col1", Type: "INTEGER", Value: int64(1)},
//					pkAndMeta{Name: "col2", Type: "INTEGER", Value: int64(2)},
//				},
//				{
//					pkAndMeta{Name: "col1", Type: "INTEGER", Value: int64(1)},
//					pkAndMeta{Name: "col2", Type: "INTEGER", Value: int64(2)},
//				},
//			},
//		},
//		{
//			desc:  "Update with Different Key",
//			setup: defaultSetup,
//			change: SQLiteWatchChange{
//				Op:           sqlite3.SQLITE_UPDATE,
//				DatabaseName: "main",
//				TableName:    "table1",
//				OldRowID:     0,
//				NewRowID:     0,
//				OldRow:       []interface{}{int64(1), int64(2), int64(3)},
//				NewRow:       []interface{}{int64(1), int64(3), int64(3)},
//			},
//			expectKeys: [][]interface{}{
//				{
//					pkAndMeta{Name: "col1", Type: "INTEGER", Value: int64(1)},
//					pkAndMeta{Name: "col2", Type: "INTEGER", Value: int64(2)},
//				},
//				{
//					pkAndMeta{Name: "col1", Type: "INTEGER", Value: int64(1)},
//					pkAndMeta{Name: "col2", Type: "INTEGER", Value: int64(3)},
//				},
//			},
//		},
//		{
//			desc:  "Delete",
//			setup: defaultSetup,
//			change: SQLiteWatchChange{
//				Op:           sqlite3.SQLITE_DELETE,
//				DatabaseName: "main",
//				TableName:    "table1",
//				OldRowID:     0,
//				NewRowID:     0,
//				OldRow:       []interface{}{int64(1), int64(2), int64(3)},
//				NewRow:       nil,
//			},
//			expectKeys: [][]interface{}{
//				{
//					pkAndMeta{Name: "col1", Type: "INTEGER", Value: int64(1)},
//					pkAndMeta{Name: "col2", Type: "INTEGER", Value: int64(2)},
//				},
//			},
//		},
//		{
//			desc: "byte slice to string conversion",
//			setup: `
//				CREATE TABLE table1 (
//					col1 INTEGER,
//					col2 VARCHAR,
//					col3 INTEGER,
//					PRIMARY KEY(col1, col2)
//				);`,
//			change: SQLiteWatchChange{
//				Op:           sqlite3.SQLITE_INSERT,
//				DatabaseName: "main",
//				TableName:    "table1",
//				OldRowID:     0,
//				NewRowID:     0,
//				OldRow:       nil,
//				NewRow:       []interface{}{int64(1), "test value", int64(3)},
//			},
//			expectKeys: [][]interface{}{
//				{
//					pkAndMeta{Name: "col1", Type: "INTEGER", Value: int64(1)},
//					pkAndMeta{Name: "col2", Type: "VARCHAR", Value: "test value"},
//				},
//			},
//		},
//	}
//
//	for _, testCase := range suite {
//		t.Run(testCase.desc, func(t *testing.T) {
//			db, err := sql.Open("sqlite3", ":memory:")
//			if err != nil {
//				t.Fatalf("Unexpected error: %v", err)
//			}
//			defer db.Close()
//
//			_, err = db.Exec(testCase.setup)
//			if err != nil {
//				t.Fatalf("Unexpected error: %v", err)
//			}
//
//			keys, err := testCase.change.ExtractKeys(db)
//			if err != nil {
//				t.Fatalf("Unexpected error: %v", err)
//			}
//
//			if diff := cmp.Diff(testCase.expectKeys, keys); diff != "" {
//				t.Errorf("Keys differ\n%v", diff)
//			}
//		})
//	}
//}
