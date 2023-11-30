package ldbwriter

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"

	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlite"
)

// Statement to update the sequence tracker, ensuring that it doesn't go
// backwards without a round-trip to the DB and/or any race conditions.
// The statement is parameterized with the only one being the new sequence
type LDBWriter interface {
	ApplyDMLStatement(ctx context.Context, statement schema.DMLStatement) error
}

type LDBWriteCallback interface {
	LDBWritten(ctx context.Context, data LDBWriteMetadata)
}

// LDBWriteMetadata contains the metadata about a statement that was written
// to the LDB.
type LDBWriteMetadata struct {
	DB        *sql.DB
	Statement schema.DMLStatement
	Changes   []sqlite.SQLiteWatchChange
}

// ldbWriter applies statements to a SQL database
type SqlLdbWriter struct {
	Db       *sql.DB
	LedgerTx *sql.Tx
	// uniquely identify this SqlWriter
	Logger *events.Logger
	ID     string
}

// Applies a DML statement to the writer's db, updating the sequence
// tracking table in the same transaction
func (w *SqlLdbWriter) ApplyDMLStatement(_ context.Context, statement schema.DMLStatement) error {
	var tx *sql.Tx
	var err error

	stats.Incr("sql_ldb_writer.apply", stats.T("id", w.ID))

	// Fill in the tx var
	if w.LedgerTx == nil {
		// Not applying a ledger transaction, so need a local transaction
		tx, err = w.Db.Begin()
		if err != nil {
			errs.Incr("sql_ldb_writer.begin_tx.error", stats.T("id", w.ID))
			return errors.Wrap(err, "open tx error")
		}
	} else {
		// Applying a ledger transaction, so bring it into scope
		tx = w.LedgerTx
	}
	logger := w.logger()

	// Handle begin ledger transaction control statements
	if statement.Statement == schema.DMLTxBeginKey {
		if w.LedgerTx != nil {
			// Attempted to open a transaction without committing the last one,
			// which is a violation of our invariants. Something is very, very
			// wrong with the ledger processing.
			tx.Rollback()
			errs.Incr("sql_ldb_writer.ledgerTx.begin_invariant_violation", stats.T("id", w.ID))
			return errors.New("invariant violation")
		}
		w.LedgerTx = tx
		logger.Debug("Begin TX at %{sequence}v", statement.Sequence)
	}

	// Update the last update table.  This will allow the ldb reader
	// the ability to calculate how up to date the ldb is by
	// subtracting wall time from that value.
	qs := fmt.Sprintf(
		"REPLACE INTO %s (name, timestamp) VALUES (?, ?)",
		ldb.LDBLastUpdateTableName)
	_, err = tx.Exec(qs, ldb.LDBLastLedgerUpdateColumn, statement.Timestamp)
	if err != nil {
		tx.Rollback()
		errs.Incr("sql_ldb_writer.upsert_last_update.error", stats.T("id", w.ID))
		return errors.Wrap(err, "update last_update")
	}

	// Update the sequence tracker row. This SQL will insert the row
	// if it doesn't exist (NOT EXISTS *) or replace the row IF the
	// current seq is < the new seq. If there are zero rows affected,
	// it means that the current seq is >= the new seq. This protects
	// against replays.
	qs = fmt.Sprintf(
		"INSERT OR REPLACE INTO %[1]s (id, seq) "+
			"SELECT %[2]d, $1 WHERE "+
			"(NOT EXISTS (SELECT * FROM %[1]s WHERE id = %[2]d)) OR "+
			"((SELECT seq FROM %[1]s WHERE id = %[2]d) < $1)",
		ldb.LDBSeqTableName,
		ldb.LDBSeqTableID)
	res, err := tx.Exec(qs, statement.Sequence.Int())
	if err != nil {
		tx.Rollback()
		errs.Incr("sql_ldb_writer.upsert_seq.error", stats.T("id", w.ID))
		return errors.Wrap(err, "update seq tracker error")
	}

	// Check for replayed statements
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		errs.Incr("sql_ldb_writer.upsert_seq.rows_affected_error", stats.T("id", w.ID))
		return errors.Wrap(err, "update seq tracker rows affected error")
	}
	if rowsAffected == 0 {
		tx.Rollback()
		errs.Incr("sql_ldb_writer.upsert_seq.replay_detected", stats.T("id", w.ID))
		return errors.New("update seq tracker replay detected")
	}

	// Nothing left to do for begin ledger control statements. The reason
	// this return isn't in the first if statement for this predicate is
	// that it allows the sequence tracker row to be updated after the
	// transaction is opened.
	if statement.Statement == schema.DMLTxBeginKey {
		return nil
	}

	// Handle end ledger transaction control statements
	if statement.Statement == schema.DMLTxEndKey {
		if w.LedgerTx == nil {
			// Attempted to commit a transaction when there is no transaction
			// open, which is a violation of our invariants. Something is very,
			// very wrong with the ledger processing!
			tx.Rollback()
			errs.Incr("sql_ldb_writer.ledgerTx.end_invariant_violation", stats.T("id", w.ID))
			return errors.New("invariant violation")
		}

		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			errs.Incr("sql_ldb_writer.ledgerTx.commit.error", stats.T("id", w.ID))
			logger.Log("Failed to commit Tx at seq %{seq}s: %{error}+v",
				statement.Sequence,
				err)
			return errors.Wrap(err, "commit multi-statement dml tx error")
		}
		stats.Incr("sql_ldb_writer.ledgerTx.commit.success", stats.T("id", w.ID))
		logger.Debug("Committed TX at %{sequence}v", statement.Sequence)
		w.LedgerTx = nil
		return nil
	}

	// Execute non-control statements
	_, err = tx.Exec(statement.Statement)
	if err != nil {
		tx.Rollback()
		errs.Incr("sql_ldb_writer.exec.error", stats.T("id", w.ID))
		return errors.Wrap(err, "exec dml statement error")
	}

	stats.Incr("sql_ldb_writer.exec.success", stats.T("id", w.ID))

	logger.Debug("Applying DML[%{sequence}d]: '%{statement}s'",
		statement.Sequence,
		statement.Statement)

	// Commit if not inside a ledger transaction, since that would be
	// a single statement transaction.
	if w.LedgerTx == nil {
		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			errs.Incr("sql_ldb_writer.single.commit.error", stats.T("id", w.ID))
			errs.Incr("sql_ldb_writer.commit.error", stats.T("id", w.ID))
			return errors.Wrap(err, "commit one-statement dml tx error")
		}
	}

	stats.Incr("sql_ldb_writer.commit.success", stats.T("id", w.ID))

	return nil
}

func (w *SqlLdbWriter) Close() error {
	if w.LedgerTx != nil {
		w.LedgerTx.Rollback()
		w.LedgerTx = nil
	}
	return nil
}

// PragmaWALResult https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
type PragmaWALResult struct {
	// 0 indicates not busy, checkpoint was not blocked from completing. 1 is blocked
	Busy int
	// number of modified pages that have been written to the write-ahead log file per a checkpoint run
	Log int
	// number of pages in the write-ahead log file that have been successfully moved back into the database file at the conclusion of the checkpoint
	Checkpointed int
	// Type of checkpointing performed
	Type CheckpointType
}

func (p *PragmaWALResult) String() string {
	return fmt.Sprintf("busy=%d, log=%d, checkpointed=%d", p.Busy, p.Log, p.Checkpointed)
}

// CheckpointType see https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
type CheckpointType string

var (
	Passive  CheckpointType = "PASSIVE"
	Full     CheckpointType = "FULL"
	Restart  CheckpointType = "RESTART"
	Truncate CheckpointType = "TRUNCATE"
)

// Checkpoint initiates a wal checkpoint, returning stats on the checkpoint's progress
// see https://www.sqlite.org/pragma.html#pragma_wal_checkpoint for more details
// requires write access
func (w *SqlLdbWriter) Checkpoint(checkpointingType CheckpointType) (*PragmaWALResult, error) {
	res, err := w.Db.Query(fmt.Sprintf("PRAGMA wal_checkpoint(%s)", string(checkpointingType)))
	if err != nil {
		w.logger().Log("error in checkpointing, %{error}", err)
		errs.Incr("sql_ldb_writer.wal_checkpoint.query.error", stats.T("id", w.ID))
		return nil, err
	}

	defer res.Close()
	var p PragmaWALResult
	if res.Next() {
		err := res.Scan(&p.Busy, &p.Log, &p.Checkpointed)
		if err != nil {
			w.logger().Log("error in scanning checkpointing, %{error}")
			errs.Incr("sql_ldb_writer.wal_checkpoint.scan.error", stats.T("id", w.ID))
			return nil, err
		}
	}
	p.Type = checkpointingType
	return &p, nil
}

func (w *SqlLdbWriter) logger() *events.Logger {
	if w.Logger == nil {
		w.Logger = events.DefaultLogger
	}
	return w.Logger
}
