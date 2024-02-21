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

func TestPrepareFamilyString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Single family",
			input:    "family1",
			expected: "('family1')",
		},
		{
			name:     "Multiple families",
			input:    "family1,family2,family3",
			expected: "('family1', 'family2', 'family3')",
		},
		{
			name:     "No families",
			input:    "",
			expected: "('')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := prepareFamilyString(tt.input)
			if output != tt.expected {
				t.Errorf("prepareFamilyString(%q) = %q, want %q", tt.input, output, tt.expected)
			}
		})
	}
}
