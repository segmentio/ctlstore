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

func (w *CallbackWriter) ApplyDMLStatement(ctx context.Context, statement schema.DMLStatement) error {
	err := w.Delegate.ApplyDMLStatement(ctx, statement)
	if err != nil {
		return err
	}

	// If beginning a transaction then start accumulating changes
	if statement.Statement == schema.DMLTxBeginKey {
		w.transactionChanges = make([]sqlite.SQLiteWatchChange, 0)
		stats.Set("ldb_changes_accumulated", len(w.transactionChanges))
		return nil
	}

	changes := w.ChangeBuffer.Pop()

	// Are we in a transaction?
	if w.transactionChanges != nil {
		if statement.Statement == schema.DMLTxEndKey {
			// Transaction done! Send out the accumulated changes
			changes = append(w.transactionChanges, changes...)
			stats.Set("ldb_changes_accumulated", len(changes))
			w.transactionChanges = nil
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
