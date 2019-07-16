package ctlstore

import (
	"database/sql"

	"github.com/segmentio/ctlstore/pkg/scanfunc"
	"github.com/segmentio/ctlstore/pkg/schema"
)

// Rows composes an *sql.Rows and allows scanning ctlstore table rows into
// structs or maps, similar to how the GetRowByKey reader method works.
//
// The contract around Next/Err/Close is the same was it is for
// *sql.Rows.
type Rows struct {
	rows *sql.Rows
	cols []schema.DBColumnMeta
}

// Next returns true if there's another row available.
func (r *Rows) Next() bool {
	if r.rows == nil {
		return false
	}
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
