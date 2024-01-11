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
	// Buffer between SQLite Callback and our code
	ChangeBuffer *sqlite.SQLChangeBuffer
	// Accumulated changes across multiple ApplyDMLStatement calls
	transactionChanges []sqlite.SQLiteWatchChange
}

// TODO: write a small struct with a couple receiver methods to make the below code more clean & simple

func (w *CallbackWriter) ApplyDMLStatement(ctx context.Context, statement schema.DMLStatement) error {
	err := w.Delegate.ApplyDMLStatement(ctx, statement)
	if err != nil {
		return err
	}

	// If beginning a transaction then start accumulating changes
	if statement.Statement == schema.DMLTxBeginKey {
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
		return nil
	}

	changes := w.ChangeBuffer.Pop()

	// Are we in a transaction?
	if w.transactionChanges != nil {
		if statement.Statement == schema.DMLTxEndKey {
			// Transaction done! Send out the accumulated changes
			changes = append(w.transactionChanges, changes...)
			stats.Set("ldb_changes_accumulated", len(changes))
			// Reset to size 0, but keep the underlying array
			w.transactionChanges = w.transactionChanges[:0]
		} else {
			// Transaction isn't over yet, save the latest changes
			w.transactionChanges = append(w.transactionChanges, changes...)
			stats.Set("ldb_changes_accumulated", len(w.transactionChanges))
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
