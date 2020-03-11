package ldbwriter

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlite"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"
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
}

// Applies a DML statement to the writer's db, updating the sequence
// tracking table in the same transaction
func (writer *SqlLdbWriter) ApplyDMLStatement(_ context.Context, statement schema.DMLStatement) error {
	var tx *sql.Tx
	var err error

	stats.Incr("sql_ldb_writer.apply")

	// Fill in the tx var
	if writer.LedgerTx == nil {
		// Not applying a ledger transaction, so need a local transaction
		tx, err = writer.Db.Begin()
		if err != nil {
			errs.Incr("sql_ldb_writer.begin_tx.error")
			return errors.Wrap(err, "open tx error")
		}
	} else {
		// Applying a ledger transaction, so bring it into scope
		tx = writer.LedgerTx
	}

	// Handle begin ledger transaction control statements
	if statement.Statement == schema.DMLTxBeginKey {
		if writer.LedgerTx != nil {
			// Attempted to open a transaction without committing the last one,
			// which is a violation of our invariants. Something is very, very
			// wrong with the ledger processing.
			tx.Rollback()
			errs.Incr("sql_ldb_writer.ledgerTx.begin_invariant_violation")
			return errors.New("invariant violation")
		}
		writer.LedgerTx = tx
		events.Debug("Begin TX at %{sequence}v", statement.Sequence)
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
		errs.Incr("sql_ldb_writer.upsert_last_update.error")
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
		errs.Incr("sql_ldb_writer.upsert_seq.error")
		return errors.Wrap(err, "update seq tracker error")
	}

	// Check for replayed statements
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		errs.Incr("sql_ldb_writer.upsert_seq.rows_affected_error")
		return errors.Wrap(err, "update seq tracker rows affected error")
	}
	if rowsAffected == 0 {
		tx.Rollback()
		errs.Incr("sql_ldb_writer.upsert_seq.replay_detected")
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
		if writer.LedgerTx == nil {
			// Attempted to commit a transaction when there is no transaction
			// open, which is a violation of our invariants. Something is very,
			// very wrong with the ledger processing!
			tx.Rollback()
			errs.Incr("sql_ldb_writer.ledgerTx.end_invariant_violation")
			return errors.New("invariant violation")
		}

		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			errs.Incr("sql_ldb_writer.ledgerTx.commit.error")
			events.Log("Failed to commit Tx at seq %{seq}s: %{error}+v",
				statement.Sequence,
				err)
			return errors.Wrap(err, "commit multi-statement dml tx error")
		}
		stats.Incr("sql_ldb_writer.ledgerTx.commit.success")
		events.Debug("Committed TX at %{sequence}v", statement.Sequence)
		writer.LedgerTx = nil
		return nil
	}

	// Execute non-control statements
	_, err = tx.Exec(statement.Statement)
	if err != nil {
		tx.Rollback()
		errs.Incr("sql_ldb_writer.exec.error")
		return errors.Wrap(err, "exec dml statement error")
	}

	stats.Incr("sql_ldb_writer.exec.success")

	events.Debug("Applying DML[%{sequence}d]: '%{statement}s'",
		statement.Sequence,
		statement.Statement)

	// Commit if not inside a ledger transaction, since that would be
	// a single statement transaction.
	if writer.LedgerTx == nil {
		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			errs.Incr("sql_ldb_writer.single.commit.error")
			errs.Incr("sql_ldb_writer.commit.error")
			return errors.Wrap(err, "commit one-statement dml tx error")
		}
	}

	stats.Incr("sql_ldb_writer.commit.success")

	return nil
}

func (writer *SqlLdbWriter) Close() error {
	if writer.LedgerTx != nil {
		writer.LedgerTx.Rollback()
		writer.LedgerTx = nil
	}
	return nil
}
