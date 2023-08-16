package ldbwriter

import (
	"context"
	"errors"

	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/schema"
)

type LDBMultiWriter struct {
	LdbWriters []LDBWriter
}

func NewMultiWriter(writers ...LDBWriter) *LDBMultiWriter {
	return &LDBMultiWriter{LdbWriters: writers}
}

func (mw *LDBMultiWriter) ApplyDMLStatement(_ context.Context, statement schema.DMLStatement) error {

	applyErrors := make([]error, 0, len(mw.LdbWriters))
	for _, w := range mw.LdbWriters {
		err := w.ApplyDMLStatement(context.Background(), statement)
		if err != nil {
			errs.Incr("multi_ldb_writer.apply_error")
			applyErrors = append(applyErrors, err)
		}
	}

	if len(applyErrors) > 0 {
		return errors.Join(applyErrors...)
	}
	return nil
}
