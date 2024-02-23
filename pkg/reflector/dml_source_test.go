package reflector

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/sqlgen"
	"github.com/stretchr/testify/require"
)

type sqlDmlSourceTestUtil struct {
	db *sql.DB
	t  *testing.T
}

func (u *sqlDmlSourceTestUtil) InitializeDB() {
	_, err := u.db.Exec(sqlgen.SqlSprintf(`
		CREATE TABLE ctlstore_dml_ledger (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			leader_ts INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,
			statement VARCHAR($1),
		    family_name VARCHAR(191) NOT NULL DEFAULT '',
		    table_name VARCHAR(191) NOT NULL DEFAULT ''
	 	);
	 	INSERT INTO ctlstore_dml_ledger (statement) VALUES('');
		DELETE FROM ctlstore_dml_ledger;
	`, fmt.Sprintf("%d", limits.LimitMaxDMLSize)))

	// the above bumps seq to 1 as starting value, since zero-values should
	// probably be avoided
	if err != nil {
		u.t.Fatalf("Failed to initialize DML Source DB, error: %v", err)
	}
}

func (u *sqlDmlSourceTestUtil) AddStatement(statement string) string {
	_, err := u.db.Exec("INSERT INTO ctlstore_dml_ledger (statement) VALUES(?)", statement)
	if err != nil {
		u.t.Fatalf("Failed to insert statement %v, error: %v", statement, err)
	}
	return statement
}

func (u *sqlDmlSourceTestUtil) AddStatementWithFamilyAndTable(statement, familyName, tableName string) string {
	_, err := u.db.Exec("INSERT INTO ctlstore_dml_ledger (statement, family_name, table_name) VALUES(?, ?, ?)", statement, familyName, tableName)
	if err != nil {
		u.t.Fatalf("Failed to insert statement %v with family %v and table %v, error: %v", statement, familyName, tableName, err)
	}
	return statement
}

func TestSqlDmlSource(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	srcutil := &sqlDmlSourceTestUtil{db: db, t: t}
	srcutil.InitializeDB()

	queryBlockSize := 5
	src := sqlDmlSource{
		db:              db,
		ledgerTableName: "ctlstore_dml_ledger",
		queryBlockSize:  queryBlockSize,
	}

	_, err = src.Next(ctx)
	require.Equal(t, errNoNewStatements, err)

	var ststr string
	for i := 0; i < queryBlockSize*2; i++ {
		ststr = srcutil.AddStatement("INSERT INTO foo___bar VALUES('hi mom')")
	}

	var lastSeq int64
	for i := 0; i < queryBlockSize*2; i++ {
		st, err := src.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, ststr, st.Statement)
		require.True(t, st.Sequence.Int() > lastSeq)
		lastSeq = st.Sequence.Int()
	}

	_, err = src.Next(ctx)
	require.Equal(t, errNoNewStatements, err)

	srcutil.AddStatement("INSERT INTO foo___bar VALUES('hi bro')")

	// Context cancellation handled properly
	ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	loopCounter := 0
	src.scanLoopCallBack = func() {
		if loopCounter == 1 {
			cancel()
		}
		loopCounter++
	}
	foundError := false
	for i := 0; i < 2; i++ {
		_, err = src.Next(ctx)
		cause := errors.Cause(err)
		switch {
		case cause == nil:
		case cause == context.Canceled:
			foundError = true
			break
			// the db driver will at some point return an error with
			// the value "interrupted" instead of returning
			// context.Canceled().  Sigh.
		case cause.Error() == "interrupted":
			foundError = true
			break
		}
	}
	if !foundError {
		t.Fatal("Expected a context error or an interrupted error")
	}
}

func TestSqlDmlSourceWithSharding(t *testing.T) {
	queryBlockSize := 5

	foobar := "INSERT INTO foo___bar VALUES('hi mom')"
	foobar1 := "INSERT INTO foo___bar1 VALUES('hi mom')"
	foo1bar1 := "INSERT INTO foo1___bar1 VALUES('hi mom')"
	foo1bar := "INSERT INTO foo1___bar VALUES('hi mom')"

	statements := []struct {
		statement string
		family    string
		table     string
	}{
		{statement: foobar, family: "foo", table: "bar"},
		{statement: foobar1, family: "foo", table: "bar1"},
		{statement: foo1bar1, family: "foo1", table: "bar1"},
		{statement: foo1bar, family: "foo1", table: "bar"},
	}

	testCases := []struct {
		name              string
		shardingFamily    string
		shardingTable     string
		stContains        []string
		stNotContains     []string
		seqModContains    []int64
		seqModNotContains []int64
		expectedErr       error
	}{
		{
			name:              "Single family single table",
			shardingFamily:    "foo",
			shardingTable:     "foo___bar",
			stContains:        []string{foobar},
			stNotContains:     []string{foobar1, foo1bar1, foo1bar},
			seqModContains:    []int64{0},
			seqModNotContains: []int64{1, 2, 3},
			expectedErr:       nil,
		},
		{
			name:              "Single family multiple tables",
			shardingFamily:    "foo",
			shardingTable:     "foo___bar,foo___bar1",
			stContains:        []string{foobar, foobar1},
			stNotContains:     []string{foo1bar1, foo1bar},
			seqModContains:    []int64{0, 1},
			seqModNotContains: []int64{2, 3},
			expectedErr:       nil,
		},
		{
			name:              "Multiple families multiple tables",
			shardingFamily:    "foo,foo1",
			shardingTable:     "foo___bar,foo1___bar1",
			stContains:        []string{foobar, foo1bar1},
			stNotContains:     []string{foo1bar, foobar1},
			seqModContains:    []int64{0, 2},
			seqModNotContains: []int64{1, 3},
			expectedErr:       nil,
		},
		{
			name:              "All families all tables",
			shardingFamily:    "foo,foo1",
			shardingTable:     "foo___bar,foo___bar1,foo1___bar1,foo1___bar",
			stContains:        []string{foobar, foobar1, foo1bar1, foo1bar},
			stNotContains:     []string{},
			seqModContains:    []int64{0, 1, 2, 3},
			seqModNotContains: []int64{},
			expectedErr:       nil,
		},
		{
			name:              "No family no table",
			shardingFamily:    "",
			shardingTable:     "",
			stContains:        []string{foobar, foobar1, foo1bar1, foo1bar},
			stNotContains:     []string{},
			seqModContains:    []int64{0, 1, 2, 3},
			seqModNotContains: []int64{},
			expectedErr:       nil,
		},
		{
			name:              "Single family no table",
			shardingFamily:    "foo",
			shardingTable:     "",
			stContains:        []string{},
			stNotContains:     []string{foobar, foobar1, foo1bar1, foo1bar},
			seqModContains:    []int64{},
			seqModNotContains: []int64{0, 1, 2, 3},
			expectedErr:       nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := sql.Open("sqlite3", ":memory:")
			require.NoError(t, err)

			srcutil := &sqlDmlSourceTestUtil{db: db, t: t}
			srcutil.InitializeDB()

			src := sqlDmlSource{
				db:              db,
				ledgerTableName: "ctlstore_dml_ledger",
				shardingFamily:  tt.shardingFamily,
				shardingTable:   tt.shardingTable,
				queryBlockSize:  queryBlockSize,
			}

			_, err = src.Next(ctx)
			require.Equal(t, errNoNewStatements, err)

			for i := 0; i < queryBlockSize*len(statements); i++ {
				for j := 0; j < len(statements); j++ {
					_ = srcutil.AddStatementWithFamilyAndTable(statements[j].statement, statements[j].family, statements[j].table)
				}
			}

			var lastSeq int64
			for i := 0; i < queryBlockSize*len(statements)*len(tt.stContains); i++ {
				st, err := src.Next(ctx)
				require.NoError(t, err)
				require.Contains(t, tt.stContains, st.Statement)
				require.NotContains(t, tt.stNotContains, st.Statement)
				require.True(t, st.Sequence.Int() > lastSeq)
				lastSeq = st.Sequence.Int()
				require.Contains(t, tt.seqModContains, (lastSeq-2)%int64(len(statements)))
				require.NotContains(t, tt.seqModNotContains, (lastSeq-2)%int64(len(statements)))
			}

			_, err = src.Next(ctx)
			require.Equal(t, errNoNewStatements, err)

			srcutil.AddStatement("INSERT INTO foo___bar VALUES('hi bro')")

			// Context cancellation handled properly
			ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()
			loopCounter := 0
			src.scanLoopCallBack = func() {
				if loopCounter == 1 {
					cancel()
				}
				loopCounter++
			}
			foundError := false
			for i := 0; i < 2; i++ {
				_, err = src.Next(ctx)
				cause := errors.Cause(err)
				switch {
				case cause == nil:
				case cause == context.Canceled:
					foundError = true
					break
					// the db driver will at some point return an error with
					// the value "interrupted" instead of returning
					// context.Canceled().  Sigh.
				case cause.Error() == "interrupted":
					foundError = true
					break
				}
			}
			if !foundError {
				t.Fatal("Expected a context error or an interrupted error")
			}
		})
	}
}

func TestPrepareString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Single family",
			input:    "family1",
			expected: "(\"family1\")",
		},
		{
			name:     "Multiple families",
			input:    "family1,family2,family3",
			expected: "(\"family1\", \"family2\", \"family3\")",
		},
		{
			name:     "No families",
			input:    "",
			expected: "(\"\")",
		},
		{
			name:     "Sharding table",
			input:    "foo___bar",
			expected: "(\"foo___bar\")",
		},
		{
			name:     "Multiple sharding tables",
			input:    "foo___bar,foo1___bar1",
			expected: "(\"foo___bar\", \"foo1___bar1\")",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := prepareString(tt.input)
			if output != tt.expected {
				t.Errorf("prepareString(%q) = %q, want %q", tt.input, output, tt.expected)
			}
		})
	}
}
