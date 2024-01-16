package ldbwriter

import (
	"context"
	"database/sql"
	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlite"
	"github.com/stretchr/testify/assert"
	"testing"
)

/*
 * Simple LDBWriteCallback handler that just stores the changes it gets.
 */
type TestUpdateCallbackHandler struct {
	Changes []sqlite.SQLiteWatchChange
}

// Record the changes propagated from ApplyDMLStatement
func (u *TestUpdateCallbackHandler) LDBWritten(ctx context.Context, data LDBWriteMetadata) {
	// The [:0] slice operation will reuse the underlying array of u.Changes if it's large enough
	// to hold all elements of data.Changes, otherwise it will allocate a new one.
	u.Changes = append(u.Changes[:0], data.Changes...)
}

func (u *TestUpdateCallbackHandler) UpdateCount() int {
	return len(u.Changes)
}

func (u *TestUpdateCallbackHandler) Reset() {
	u.Changes = u.Changes[:0]
	return
}

/*
 * Test strategy:
 * Check how many times we get callbacks while applying DML statements,
 * and how many updates we get per callback.
 */
func TestCallbackWriter_ApplyDMLStatement(t *testing.T) {
	// Begin boilerplate
	var err error
	ctx := context.Background()
	var changeBuffer sqlite.SQLChangeBuffer
	dbName := "test_ldb_callback_writer"
	_ = sqlite.RegisterSQLiteWatch(dbName, &changeBuffer)

	db, err := sql.Open(dbName, ":memory:")
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}
	defer db.Close()

	err = ldb.EnsureLdbInitialized(ctx, db)
	if err != nil {
		t.Fatalf("Couldn't initialize SQLite db, error %v", err)
	}
	// End boilerplate

	// Set up the callback writer with our test callback handler
	ldbWriteCallback := &TestUpdateCallbackHandler{}

	writer := CallbackWriter{
		DB:           db,
		Delegate:     &SqlLdbWriter{Db: db},
		Callbacks:    []LDBWriteCallback{ldbWriteCallback},
		ChangeBuffer: &changeBuffer,
	}

	err = writer.ApplyDMLStatement(ctx, schema.NewTestDMLStatement("CREATE TABLE foo (bar VARCHAR);"))
	if err != nil {
		t.Fatalf("Could not issue CREATE TABLE statements, error %v", err)
	}

	type args struct {
		ctx        context.Context
		statements []schema.DMLStatement
	}
	tests := []struct {
		name                       string
		args                       args
		expectedCallbacks          int
		expectedUpdatesPerCallback int
		wantErr                    bool
	}{
		{
			name: "Test one bare statement",
			args: args{
				ctx:        ctx,
				statements: []schema.DMLStatement{schema.NewTestDMLStatement("INSERT INTO foo VALUES('dummy');")},
			},
			expectedCallbacks:          1,
			expectedUpdatesPerCallback: 1,
			wantErr:                    false,
		},
		{
			name: "Test three bare statements",
			args: args{
				ctx: ctx,
				statements: []schema.DMLStatement{
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('boston');"),
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('detroit');"),
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('chicago');"),
				},
			},
			// bare statements outside of a transaction should get a callback each time
			expectedCallbacks:          3,
			expectedUpdatesPerCallback: 1,
			wantErr:                    false,
		},
		{
			name: "Test three statements in a ledger transaction",
			args: args{
				ctx: ctx,
				statements: []schema.DMLStatement{
					schema.NewTestDMLStatement(schema.DMLTxBeginKey),
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('asdf');"),
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('foo');"),
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('bar');"),
					schema.NewTestDMLStatement(schema.DMLTxEndKey),
				},
			},
			// since it's a transaction, we expect only one callback, and it should have all 3 updates
			expectedCallbacks:          1,
			expectedUpdatesPerCallback: 3,
			wantErr:                    false,
		},
		{
			name: "Test two statements in a ledger transaction",
			args: args{
				ctx: ctx,
				statements: []schema.DMLStatement{
					schema.NewTestDMLStatement(schema.DMLTxBeginKey),
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('blue');"),
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('green');"),
					schema.NewTestDMLStatement(schema.DMLTxEndKey),
				},
			},
			// since it's a transaction, we expect only one callback, and it should have all 2 updates
			expectedCallbacks:          1,
			expectedUpdatesPerCallback: 2,
			wantErr:                    false,
		},
		{
			name: "Test four statements in a ledger transaction, using REPLACE INTO",
			args: args{
				ctx: ctx,
				statements: []schema.DMLStatement{
					schema.NewTestDMLStatement(schema.DMLTxBeginKey),
					schema.NewTestDMLStatement("REPLACE INTO foo('bar') VALUES('lion');"),
					schema.NewTestDMLStatement("REPLACE INTO foo('bar') VALUES('green');"),
					schema.NewTestDMLStatement("REPLACE INTO foo('bar') VALUES('boston');"),
					schema.NewTestDMLStatement("REPLACE INTO foo('bar') VALUES('detroit')"),
					schema.NewTestDMLStatement(schema.DMLTxEndKey),
				},
			},
			// since it's a transaction, we expect only one callback, and it should have all 4 updates
			expectedCallbacks:          1,
			expectedUpdatesPerCallback: 4,
			wantErr:                    false,
		},
		{
			name: "Test six statements in two ledger transactions",
			args: args{
				ctx: ctx,
				statements: []schema.DMLStatement{
					schema.NewTestDMLStatement(schema.DMLTxBeginKey),
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('cat');"),
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('dog');"),
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('hamster');"),
					schema.NewTestDMLStatement(schema.DMLTxEndKey),
					schema.NewTestDMLStatement(schema.DMLTxBeginKey),
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('fish');"),
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('rabbit');"),
					schema.NewTestDMLStatement("INSERT INTO foo VALUES('ferret');"),
					schema.NewTestDMLStatement(schema.DMLTxEndKey),
				},
			},
			// since it's a transaction, we expect only one callback, and it should have all 3 updates
			expectedCallbacks:          2,
			expectedUpdatesPerCallback: 3,
			wantErr:                    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callbackCount := 0
			for _, statement := range tt.args.statements {
				if err := writer.ApplyDMLStatement(tt.args.ctx, statement); (err != nil) != tt.wantErr {
					t.Errorf("ApplyDMLStatement() error = %v, wantErr %v", err, tt.wantErr)
				}
				// did we get a callback from that statement being applied?
				if ldbWriteCallback.UpdateCount() > 0 {
					callbackCount++
					assert.Equal(t, tt.expectedUpdatesPerCallback, ldbWriteCallback.UpdateCount())
					// delete previous callback's update entries since we "handled" the callback
					ldbWriteCallback.Reset()
				}
			}
			assert.Equal(t, tt.expectedCallbacks, callbackCount)
		})
	}
}
