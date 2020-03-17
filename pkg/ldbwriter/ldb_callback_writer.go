package ldbwriter

import (
	"context"
	"database/sql"

	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlite"
	"github.com/segmentio/events/v2"
)

// CallbackWriter is an LDBWriter that delegates to another
// writer and then, upon a successful write, executes N callbacks.
type CallbackWriter struct {
	DB           *sql.DB
	Delegate     LDBWriter
	Callbacks    []LDBWriteCallback
	ChangeBuffer *sqlite.SQLChangeBuffer
}

func (w *CallbackWriter) ApplyDMLStatement(ctx context.Context, statement schema.DMLStatement) error {
	err := w.Delegate.ApplyDMLStatement(ctx, statement)
	if err != nil {
		return err
	}
	changes := w.ChangeBuffer.Pop()
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
