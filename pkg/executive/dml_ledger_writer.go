package executive

import (
	"context"
	"database/sql"

	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlgen"
	"github.com/segmentio/stats"
)

// Writes DML entries to log table within an existing transaction. Make
// sure to call Close() after finishing.
type dmlLedgerWriter struct {
	Tx        *sql.Tx
	TableName string
	_stmt     *sql.Stmt
}

func (w *dmlLedgerWriter) BeginTx(ctx context.Context) (seq schema.DMLSequence, err error) {
	return w.Add(ctx, schema.DMLTxBeginKey)
}

func (w *dmlLedgerWriter) CommitTx(ctx context.Context) (seq schema.DMLSequence, err error) {
	return w.Add(ctx, schema.DMLTxEndKey)
}

// Writes an entry to the DML log, returning the sequence or an error
// if any occurs.
func (w *dmlLedgerWriter) Add(ctx context.Context, statement string) (seq schema.DMLSequence, err error) {
	if w._stmt == nil {
		qs := sqlgen.SqlSprintf("INSERT INTO $1 (statement) VALUES(?)", w.TableName)
		stmt, err := w.Tx.PrepareContext(ctx, qs)
		if err != nil {
			errs.Incr("dml_ledger_writer.prepare.error")
			return 0, err
		}
		w._stmt = stmt
	}

	res, err := w._stmt.ExecContext(ctx, statement)
	if err != nil {
		errs.Incr("dml_ledger_writer.exec.error")
		return
	}
	stats.Incr("dml_ledger_writer.exec.success")

	seqId, err := res.LastInsertId()
	if err != nil {
		errs.Incr("dml_ledger_writer.last_insert_id_error")
		return
	}

	seq = schema.DMLSequence(seqId)
	return
}

func (w *dmlLedgerWriter) Close() error {
	if w._stmt != nil {
		return w._stmt.Close()
	}
	return nil
}
