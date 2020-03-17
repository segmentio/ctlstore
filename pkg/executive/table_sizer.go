package executive

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/utils"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"
)

const (
	tableSizerRefreshTimeout = 10 * time.Second // how frequently to poll the db
)

type (
	// tableSizer async loads table sizes from the ctldb (except for sqlite3) and allows
	// the client to query whether or not tables have exceeded their max size using the
	// tableOK() method.  If the database type is sqlite3, then the tableSizer will be
	// disabled and most methods will be no-ops.
	tableSizer struct {
		enabled                 bool
		ctldb                   *sql.DB
		schema                  string
		pollPeriod              time.Duration
		tableSizes              map[schema.FamilyTable]int64
		defaultTableLimit       limits.SizeLimits
		configuredMaxTableSizes map[schema.FamilyTable]limits.SizeLimits
		mut                     sync.Mutex
	}
)

func newTableSizer(ctldb *sql.DB, dbType string, defaultTableLimit limits.SizeLimits, pollPeriod time.Duration) *tableSizer {
	// the table sizer does not work for sqlite3 databases.
	enabled := dbType != "sqlite3"
	if !enabled {
		events.Log("Table sizer is disabled due to dbType=%s", dbType)
	}
	return &tableSizer{
		enabled:                 enabled,
		ctldb:                   ctldb,
		pollPeriod:              pollPeriod,
		defaultTableLimit:       defaultTableLimit,
		tableSizes:              make(map[schema.FamilyTable]int64), // keyed by full table name
		configuredMaxTableSizes: make(map[schema.FamilyTable]limits.SizeLimits),
	}
}

// tableOK returns true/false based on whether or no the table is known to the sizer.
// an error will be returned if the table has exceeded its max size limit.
func (s *tableSizer) tableOK(ft schema.FamilyTable) (found bool, err error) {
	if !s.enabled {
		return false, nil
	}
	s.mut.Lock()
	tableSize, tableFound := s.tableSizes[ft]
	defaultLimit := s.defaultTableLimit
	configuredLimit, tableSizeLimitFound := s.configuredMaxTableSizes[ft]
	s.mut.Unlock()

	if !tableFound {
		// we don't know about it yet. this is normal when a new table has been created
		// and the sizer has not yet refreshed its table size map.
		events.Debug("received table size check about unknown table '%s'", ft)
		errs.Incr("table-sizer-unknown-table", ft.Tag())
		return false, nil
	}
	found = true
	maxSize := defaultLimit.MaxSize
	warnSize := defaultLimit.WarnSize
	if tableSizeLimitFound {
		maxSize = configuredLimit.MaxSize
		warnSize = configuredLimit.WarnSize
	}
	switch {
	case tableSize > maxSize:
		errs.Incr("table-size-overage", ft.Tag())
		return found, &errs.InsufficientStorageErr{Err: fmt.Sprintf("table '%s' has exceeded the max size of %d", ft, maxSize)}
	case tableSize > warnSize:
		stats.Incr("table-size-warning", ft.Tag())
		return found, nil
	default:
		return found, nil
	}
}

// start performs one update synchronously, and then starts updating every
// poll period.
func (s *tableSizer) start(ctx context.Context) error {
	if !s.enabled {
		events.Log("Table sizer not starting b/c it is disabled")
		return nil
	}
	events.Log("starting table sizer with a period of %v", s.pollPeriod)
	doRefresh := func() error {
		err := s.refresh(ctx)
		if err != nil {
			errs.IncrDefault(stats.Tag{Name: "op", Value: "refresh-table-sizer"})
		}
		return err
	}
	if err := doRefresh(); err != nil {
		return err
	}
	go utils.CtxLoop(ctx, s.pollPeriod, func() {
		if err := doRefresh(); err != nil {
			events.Log("could not refresh table sizer: %{err}v", err)
		}
	})
	return nil
}

// refresh updates the current sizes and also the configured table limits
func (s *tableSizer) refresh(ctx context.Context) error {
	if !s.enabled {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, tableSizerRefreshTimeout)
	defer cancel()
	sizes, err := s.getSizes(ctx)
	if err != nil {
		return errors.Wrap(err, "get table sizes")
	}
	configuredLimits, err := s.getLimits(ctx)
	if err != nil {
		return errors.Wrap(err, "get configured table limits")
	}
	s.mut.Lock()
	defer s.mut.Unlock()
	s.tableSizes = sizes
	s.configuredMaxTableSizes = configuredLimits
	return nil
}

// getLimits refreshes the table size limits that are configured in the db
func (s *tableSizer) getLimits(ctx context.Context) (map[schema.FamilyTable]limits.SizeLimits, error) {
	query := "SELECT family_name, table_name, max_size_bytes, warn_size_bytes FROM max_table_sizes"
	rows, err := s.ctldb.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := make(map[schema.FamilyTable]limits.SizeLimits)
	for rows.Next() {
		var ft schema.FamilyTable
		var limit limits.SizeLimits
		if err := rows.Scan(&ft.Family, &ft.Table, &limit.MaxSize, &limit.WarnSize); err != nil {
			return nil, err
		}
		res[ft] = limit
	}
	return res, rows.Err()
}

// getSizes fetches the current sizes of tables in the ctldb. currently
// it just queries the ctldb's schema tables, but we also may want to query
// ldb sizes too in the future.
func (s *tableSizer) getSizes(ctx context.Context) (map[schema.FamilyTable]int64, error) {
	if !s.enabled {
		return nil, nil
	}
	dbSchema, err := s.getSchema(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get schema")
	}
	query := "SELECT table_name, (data_length + index_length) FROM information_schema.tables WHERE table_schema=?"
	rows, err := s.ctldb.QueryContext(ctx, query, dbSchema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := make(map[schema.FamilyTable]int64)
	for rows.Next() {
		var name string
		var amount int64
		if err := rows.Scan(&name, &amount); err != nil {
			return nil, err
		}
		ft, ok := schema.ParseFamilyTable(name)
		if !ok {
			continue
		}
		stats.Set("table-sizes", amount, stats.T("family", ft.Family), stats.T("table", ft.Table))
		res[ft] = amount
		events.Debug("table sizer: %v=%v", name, amount)
	}
	return res, rows.Err()
}

// getSchema attempts to find the current schema, and memoizes the result
func (s *tableSizer) getSchema(ctx context.Context) (string, error) {
	s.mut.Lock()
	schema := s.schema
	s.mut.Unlock()
	if schema != "" {
		return schema, nil
	}
	row := s.ctldb.QueryRowContext(ctx, "select database()")
	if err := row.Scan(&schema); err != nil {
		return "", err
	}
	s.mut.Lock()
	s.schema = schema
	s.mut.Unlock()
	return schema, nil
}
