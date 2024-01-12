package ldbwriter

import (
	"context"
	"database/sql"

	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlite"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"
)

// CallbackWriter is an LDBWriter that delegates to another
// writer and then, upon a successful write, executes N callbacks.
type CallbackWriter struct {
	DB        *sql.DB
	Delegate  LDBWriter
	Callbacks []LDBWriteCallback
	// Buffer between SQLite preupdate Hook and this code
	ChangeBuffer *sqlite.SQLChangeBuffer
	// Accumulated changes across multiple ApplyDMLStatement calls
	transactionChanges []sqlite.SQLiteWatchChange
}

func (w *CallbackWriter) InTransaction() bool {
	return len(w.transactionChanges) > 0
}

func (w *CallbackWriter) BeginTransaction() {
	if w.transactionChanges == nil {
		w.transactionChanges = make([]sqlite.SQLiteWatchChange, 0)
	} else {
		if len(w.transactionChanges) > 0 {
			// This should never happen, but just in case...
			stats.Add("ldb_changes_abandoned", len(w.transactionChanges))
			events.Log("error: abandoned %{count}d changes from incomplete transaction", len(w.transactionChanges))
		}
		// Reset to size 0, but keep the underlying array
		w.transactionChanges = w.transactionChanges[:0]
	}
	stats.Set("ldb_changes_accumulated", 0)
}

// Transaction done! Return the accumulated changes including the latest ones
func (w *CallbackWriter) EndTransaction(changes *[]sqlite.SQLiteWatchChange) {
	*changes = append(w.transactionChanges, *changes...)
	stats.Set("ldb_changes_accumulated", len(*changes))
	// Reset to size 0, but keep the underlying array
	w.transactionChanges = w.transactionChanges[:0]
}

// Transaction isn't over yet, save the latest changes
func (w *CallbackWriter) AccumulateChanges(changes []sqlite.SQLiteWatchChange) {
	w.transactionChanges = append(w.transactionChanges, changes...)
	stats.Set("ldb_changes_accumulated", len(w.transactionChanges))
}

// ApplyDMLStatement
//
// It is not obvious, but this code executes synchronously:
//  1. Delegate.AppyDMLStatement executes the DML statement against the SQLite LDB.
//     (⚠️ WARNING: That's what the code is wired up to do today, January 2024, though the Delegate
//     could be doing other things since the code is so flexible.)
//  2. When SQLite processes the statement it invokes our preupdate hook (see sqlite_watch.go).
//  3. Our preupdate hook writes the changes to the change buffer.
//  4. The code returns here, and we decide whether to process the change buffer immediately or
//     wait until the end of the ledger transaction.
func (w *CallbackWriter) ApplyDMLStatement(ctx context.Context, statement schema.DMLStatement) error {
	err := w.Delegate.ApplyDMLStatement(ctx, statement)
	if err != nil {
		return err
	}

	// If beginning a transaction then start accumulating changes, don't send them out yet
	if statement.Statement == schema.DMLTxBeginKey {
		w.BeginTransaction()
		return nil
	}

	changes := w.ChangeBuffer.Pop()

	if w.InTransaction() {
		if statement.Statement == schema.DMLTxEndKey {
			// Transaction done, let's send what we have accumulated
			w.EndTransaction(&changes)
		} else {
			// Transaction not over, continue accumulating
			w.AccumulateChanges(changes)
			return nil
		}
	}

	stats.Observe("ldb_changes_written", len(changes))
	for _, callback := range w.Callbacks {
		events.Debug("Writing DML callback for %{cb}T", callback)
		callback.LDBWritten(ctx, LDBWriteMetadata{
			DB:        w.DB,
			Statement: statement,
			Changes:   changes,
		})
	}
	return nil
}
