package executive

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/utils"
	"github.com/segmentio/events"
	"github.com/segmentio/stats/v4"
)

const (
	defaultRefreshPeriod        = 1 * time.Minute // how frequently to pull config data from ctldb
	defaultDeleteUsagePeriod    = 1 * time.Hour   // how frequently to delete old usage data
	defaultDeleteUsageOlderThan = 24 * time.Hour  // how far back we should delete usage data
)

type (
	// dbLimiter implements limiter
	dbLimiter struct {
		db                 *sql.DB
		tableSizer         *tableSizer
		mut                sync.Mutex // protects da maps
		defaultWriterLimit limits.RateLimit
		perWriterLimits    map[string]int64 // writer name -> max mutations per period
		timeFunc           func() time.Time
	}
	// limiterRequest represents a request to the limiter for an impending set of writes
	limiterRequest struct {
		writerName string
		familyName string
		requests   []ExecutiveMutationRequest
	}
)

func newDBLimiter(db *sql.DB, dbType string, defaultTableLimit limits.SizeLimits, writerLimitPeriod time.Duration, writerLimit int64) *dbLimiter {
	return &dbLimiter{
		db:                 db,
		tableSizer:         newTableSizer(db, dbType, defaultTableLimit, time.Minute),
		defaultWriterLimit: limits.RateLimit{Amount: writerLimit, Period: writerLimitPeriod},
		perWriterLimits:    make(map[string]int64),
	}
}

// allowed gates writes to ctldb. it ensures that the tables are not too large, and also that the
// specified writer has enough of a quota to make the writes.  Note that if there is any table in
// the limiter request that has exceeded its max size, the entire request will be rejected.  This is
// to prevent partial mutations being performed over and over due to the client retrying a failed
// request that includes some tables that are not over their limits.
func (l *dbLimiter) allowed(ctx context.Context, tx *sql.Tx, lr limiterRequest) (bool, error) {
	if err := l.checkTableSizes(ctx, lr); err != nil {
		return false, errors.Wrap(err, "check table sizes")
	}
	allowed, err := l.checkWriterRates(ctx, tx, lr)
	if err != nil {
		return false, errors.Wrap(err, "check writer rates")
	}
	return allowed, nil
}

// checkWriterRates ensures that the writer has enough of a quote in the current bucket to make writes.
//
// in order to have this work on both mysql and sqlite3, we had to forego the use of nice upsert
// syntax that is highly driver-dependent. we instead fall back to doing a read-then-write inside
// of a transaction for writer rates.
func (l *dbLimiter) checkWriterRates(ctx context.Context, tx *sql.Tx, lr limiterRequest) (bool, error) {
	numMutations := len(lr.requests)
	if numMutations == 0 {
		// no problem, we will always allow zero mutations
		return true, nil
	}
	bucket := l.periodEpoch()
	row := tx.QueryRowContext(ctx, "SELECT amount FROM writer_usage WHERE writer_name=? AND bucket=?", lr.writerName, bucket)
	var amount int64
	err := row.Scan(&amount)
	if err != nil && err != sql.ErrNoRows {
		return false, errors.Wrap(err, "select from writer_usage")
	}
	amount += int64(numMutations)
	if err == sql.ErrNoRows {
		// do an insert
		res, err := tx.ExecContext(ctx, "INSERT INTO writer_usage (bucket,writer_name,amount) VALUES (?,?,?)",
			bucket, lr.writerName, amount)
		if err != nil {
			return false, errors.Wrap(err, "insert into writer_usage")
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return false, errors.Wrap(err, "affected rows from insert into writer_usage")
		}
		if rowsAffected == 0 {
			return false, errors.New("insert into writer_usage failed (no rows updated)")
		}
	} else {
		// do an update
		res, err := tx.ExecContext(ctx, "UPDATE writer_usage SET amount=? where bucket=? and writer_name=?",
			amount, bucket, lr.writerName)
		if err != nil {
			return false, errors.Wrap(err, "update writer_usage")
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return false, errors.Wrap(err, "affected rows from update writer_usage")
		}
		if rowsAffected == 0 {
			return false, errors.New("updating writer_usage failed (no rows updated)")
		}
	}
	writerLimit := l.limitForWriter(lr.writerName)
	allowed := amount <= writerLimit
	events.Debug("limiter: writer:%v writerLimit:%v amount:%v allowed:%v", lr.writerName, writerLimit, amount, allowed)
	return allowed, nil
}

// checkTableSizes ensures that if we are over our limit for a particular table that's being
// written to, we return a non-nil error
func (l *dbLimiter) checkTableSizes(ctx context.Context, lr limiterRequest) error {
	tables := make(map[schema.FamilyTable]struct{})
	for _, req := range lr.requests {
		ft := schema.FamilyTable{Family: lr.familyName, Table: req.TableName}
		if _, ok := tables[ft]; !ok {
			tables[ft] = struct{}{} // mark this FamilyTable as having been visited
			if _, err := l.tableSizer.tableOK(ft); err != nil {
				return err
			}
		}
	}
	return nil
}

// start initializes the db limiter and spawns necessary goroutines
func (l *dbLimiter) start(ctx context.Context) error {
	events.Log("Starting the db limiter")
	if err := l.tableSizer.start(ctx); err != nil {
		return errors.Wrap(err, "could not start sizer")
	}
	instrumentUpdateErr := func(err error) {
		errs.IncrDefault(stats.Tag{Name: "op", Value: "update-limits"})
	}
	// we always require an initial update of limit config from the db
	if err := l.refreshWriterLimits(ctx); err != nil {
		instrumentUpdateErr(err)
		return errors.Wrap(err, "refresh writer limits")
	}
	// after we've done one refreshWriterLimits successfully, we'll do the rest async
	go utils.CtxLoop(ctx, defaultRefreshPeriod, func() {
		if err := l.refreshWriterLimits(ctx); err != nil {
			events.Log("could not update limits: %{error}s", err)
			instrumentUpdateErr(err)
		}
	})
	// also, periodically try to clean up the writer_usage table.
	go utils.CtxLoop(ctx, defaultDeleteUsagePeriod, func() {
		if err := l.deleteOldUsageData(ctx); err != nil {
			events.Log("could not collect garbage %{error}s", err)
			errs.IncrDefault(stats.Tag{Name: "op", Value: "limiter-collect-garbage"})
		}
	})
	return nil
}

// deleteOldUsageData cleans the writer_usage table of old entries
func (l *dbLimiter) deleteOldUsageData(ctx context.Context) error {
	deleteEpoch := l.getTime().Add(-defaultDeleteUsageOlderThan).Truncate(l.defaultWriterLimit.Period).Unix()

	res, err := l.db.ExecContext(ctx, "delete from writer_usage where bucket < ?", deleteEpoch)
	if err != nil {
		return errors.Wrap(err, "could not delete from writer_usage table")
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "could not get rows affected after deleting from writer_usage")
	}
	if rows > 0 {
		events.Log("deleted %{rows}d rows from the writer_usage table", rows)
	}
	stats.Add("writer-usage-rows-deleted", rows)
	return nil
}

// refreshWriterLimits queries the database for the current writer limits configuration
// and updates the cached values
func (l *dbLimiter) refreshWriterLimits(ctx context.Context) error {
	rows, err := l.db.QueryContext(ctx, "select writer_name, max_rows_per_minute FROM max_writer_rates")
	if err != nil {
		return errors.Wrap(err, "could not query max_writer_rates")
	}
	defer rows.Close()
	writerLimits := make(map[string]int64)
	for rows.Next() {
		var writerName string
		var maxRowsPerMinute int64
		if err = rows.Scan(&writerName, &maxRowsPerMinute); err != nil {
			return errors.Wrap(err, "could not scan max_writer_rates")
		}
		// we need to convert the max rows per minute to the rate for the period which we're checking

		rateLimit := limits.RateLimit{Amount: maxRowsPerMinute, Period: time.Minute}
		adjustedRate, err := rateLimit.AdjustAmount(l.defaultWriterLimit.Period)
		if err != nil {
			return errors.Wrap(err, "adjust found rate limit")
		}
		events.Debug("adjusted %v limit from %v/%v to %v/%v", writerName, maxRowsPerMinute, time.Minute, adjustedRate, l.defaultWriterLimit.Period)
		writerLimits[writerName] = adjustedRate
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "rows err after scanning")
	}
	// update the shared data while locked
	l.mut.Lock()
	defer l.mut.Unlock()
	l.perWriterLimits = writerLimits
	return nil
}

func (l *dbLimiter) limitForWriter(writer string) int64 {
	l.mut.Lock()
	defer l.mut.Unlock()
	if perWriterLimit, ok := l.perWriterLimits[writer]; ok {
		return perWriterLimit
	}
	return l.defaultWriterLimit.Amount
}

func (l *dbLimiter) periodEpoch() int64 {
	return l.getTime().Truncate(l.defaultWriterLimit.Period).Unix()
}

func (l *dbLimiter) getTime() time.Time {
	if l.timeFunc != nil {
		return l.timeFunc()
	}
	return time.Now()
}
