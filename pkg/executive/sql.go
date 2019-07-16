package executive

import (
	"context"
	"database/sql"
	"strings"

	_ "github.com/segmentio/go-sqlite3" // gives us sqlite3 everywhere
)

// SQLDBClient allows generalizing several database/sql types
type SQLDBClient interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

func errorIsRowConflict(err error) bool {
	return strings.Contains(err.Error(), "Duplicate entry") ||
		strings.Contains(err.Error(), "UNIQUE constraint failed")
}
