package executive

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	mysql2 "github.com/segmentio/ctlstore/pkg/mysql"
	"github.com/segmentio/ctlstore/pkg/schema"
	sqlite2 "github.com/segmentio/ctlstore/pkg/sqlite"
	sqlite "github.com/segmentio/go-sqlite3"
)

type sqlDBInfo interface {
	GetColumnInfo(ctx context.Context, tableNames []string) ([]schema.DBColumnInfo, error)
	GetAllTables(ctx context.Context) ([]schema.FamilyTable, error)
}

func getDBInfo(db *sql.DB) sqlDBInfo {
	switch t := db.Driver().(type) {
	case *mysql.MySQLDriver:
		return &mysql2.MySQLDBInfo{Db: db}
	case *sqlite.SQLiteDriver:
		return &sqlite2.SqliteDBInfo{Db: db}
	default:
		panic(fmt.Sprintf("Invalid driver type %T", t))
	}
}
