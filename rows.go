package ctlstore

import (
	"database/sql"
	"sync"
	"time"

	"github.com/segmentio/stats/v4"

	"github.com/segmentio/ctlstore/pkg/globalstats"
	"github.com/segmentio/ctlstore/pkg/scanfunc"
	"github.com/segmentio/ctlstore/pkg/schema"
)

// Rows composes an *sql.Rows and allows scanning ctlstore table rows into
// structs or maps, similar to how the GetRowByKey reader method works.
// It also keeps track of number of rows read and emits as a metric on Close
//
// The contract around Next/Err/Close is the same was it is for
// *sql.Rows.
type Rows struct {
	rows       *sql.Rows
	cols       []schema.DBColumnMeta
	familyName string
	tableName  string
	count      int
	once       sync.Once
	start      time.Time
}

// Next returns true if there's another row available.
func (r *Rows) Next() bool {
	if r.rows == nil {
		return false
	}
	r.count++
	return r.rows.Next()
}

// Err returns any error that could have been caused during
// the invocation of Next().  If Next() returns false, the caller
// must always check Err() to see if that's why iteration
// failed.
func (r *Rows) Err() error {
	if r.rows == nil {
		return nil
	}
	return r.rows.Err()
}

// Close closes the underlying *sql.Rows.
func (r *Rows) Close() error {
	if r.rows == nil {
		return nil
	}
	go r.once.Do(func() {
		globalstats.Observe("get_rows_by_key_prefix_row_count", r.count,
			stats.T("family", r.familyName),
			stats.T("table", r.tableName))
		if !r.start.IsZero() {
			globalstats.Observe("get_rows_by_prefix_scan_time", time.Now().Sub(r.start),
				stats.T("family", r.familyName),
				stats.T("table", r.tableName))
		}
	})
	return r.rows.Close()
}

// Scan deserializes the current row into the specified target.
// The target must be either a pointer to a struct, or a
// map[string]interface{}.
func (r *Rows) Scan(target interface{}) error {
	if r.rows == nil {
		return sql.ErrNoRows
	}
	scanFunc, err := scanfunc.New(target, r.cols)
	if err != nil {
		return err
	}
	return scanFunc(r.rows)
}
