package reflector

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlgen"
	"github.com/segmentio/stats/v4"
)

const (
	defaultQueryBlockSize    = 100
	dmlLedgerTimestampFormat = "2006-01-02 15:04:05"
)

var errNoNewStatements = errors.New("No new statements")

type dmlSource interface {
	Next(ctx context.Context) (schema.DMLStatement, error)
	// TODO: probably need a last sequence fetcher
}

// a dmlSource built on top of a database/sql instance
type sqlDmlSource struct {
	db               *sql.DB
	lastSequence     schema.DMLSequence
	ledgerTableName  string
	queryBlockSize   int
	buffer           []schema.DMLStatement
	scanLoopCallBack func()
}

// Next returns the next sequential statement in the source. If there are no
// new statements, it returns errNoNewStatements. Any errors that occur while
// fetching data will be returned as well.
func (source *sqlDmlSource) Next(ctx context.Context) (statement schema.DMLStatement, err error) {
	if len(source.buffer) == 0 {
		blocksize := source.queryBlockSize
		if blocksize == 0 {
			blocksize = defaultQueryBlockSize
		}

		// table layout is: seq, leader_ts, statement
		qs := sqlgen.SqlSprintf("SELECT seq, leader_ts, statement FROM $1 WHERE seq > ? ORDER BY seq LIMIT $2",
			source.ledgerTableName,
			fmt.Sprintf("%d", blocksize))

		// HMM: do we lean too hard on the LIMIT here? in the loop below
		// we'll end up spinning if the DB keeps feeding us data

		rows, err := source.db.QueryContext(ctx, qs, source.lastSequence)
		if err != nil {
			return statement, errors.Wrap(err, "select row")
		}

		// CR: reconsider naked returns here

		defer rows.Close()

		row := struct {
			seq       int64
			leaderTs  string // this is a string b/c the driver errors when trying to Scan into a *time.Time.
			statement string
		}{}

		for {
			if source.scanLoopCallBack != nil {
				source.scanLoopCallBack()
			}

			if !rows.Next() {
				break
			}

			err = rows.Scan(&row.seq, &row.leaderTs, &row.statement)
			if err != nil {
				return statement, errors.Wrap(err, "scan row")
			}

			if schema.DMLSequence(row.seq) > source.lastSequence+1 {
				stats.Incr("sql_dml_source.skipped_sequence")
			}

			timestamp, err := time.Parse(dmlLedgerTimestampFormat, row.leaderTs)
			if err != nil {
				return statement, errors.Wrapf(err, "could not parse time '%s'", row.leaderTs)
			}

			dmlst := schema.DMLStatement{
				Sequence:  schema.DMLSequence(row.seq),
				Statement: row.statement,
				Timestamp: timestamp,
			}

			source.buffer = append(source.buffer, dmlst)

			// if this doesn't get updated every time, say just doing the last row
			// after the iteration, an early return can cause lastSequence to diverge
			// from the buffer contents
			source.lastSequence = dmlst.Sequence
		}

		err = rows.Err()
		if err != nil {
			return statement, errors.Wrap(err, "rows err")
		}
	}

	// Still have to guard this case because source.buffer gets
	// mutated above, and certainly could add zero statements.
	if len(source.buffer) > 0 {
		// FIFO queue
		statement = source.buffer[0]
		source.buffer = source.buffer[1:]
		return
	}

	err = errNoNewStatements
	return
}
