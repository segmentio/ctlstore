package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/segmentio/ctlstore/pkg/scanfunc"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/go-sqlite3"
)

type (
	SQLiteWatchChange struct {
		Op           int
		DatabaseName string
		TableName    string
		OldRowID     int64
		NewRowID     int64
		OldRow       []interface{}
		NewRow       []interface{}
	}
	// pkAndMeta is a primary key value with name and type metadata to boot
	pkAndMeta struct {
		Name  string      `json:"name"`
		Type  string      `json:"type"`
		Value interface{} `json:"value"`
	}
)

// Registers a hook against dbName that will populate the passed buffer with
// sqliteWatchChange messages each time a change is executed against the
// database. These messages are pre-update, so the buffer will be populated
// before the change is committed.
func RegisterSQLiteWatch(dbName string, buffer *SQLChangeBuffer) error {
	sql.Register(dbName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			conn.RegisterPreUpdateHook(func(pud sqlite3.SQLitePreUpdateData) {
				cnt := pud.Count()
				var newRow []interface{}
				var oldRow []interface{}

				if pud.Op == sqlite3.SQLITE_UPDATE || pud.Op == sqlite3.SQLITE_DELETE {
					oldRow = make([]interface{}, cnt)
					err := pud.Old(oldRow...)
					if err != nil {
						return
					}
				}

				if pud.Op == sqlite3.SQLITE_UPDATE || pud.Op == sqlite3.SQLITE_INSERT {
					newRow = make([]interface{}, cnt)
					err := pud.New(newRow...)
					if err != nil {
						return
					}
				}

				buffer.Add(SQLiteWatchChange{
					Op:           pud.Op,
					DatabaseName: pud.DatabaseName,
					TableName:    pud.TableName,
					OldRowID:     pud.OldRowID,
					NewRowID:     pud.NewRowID,
					OldRow:       oldRow,
					NewRow:       newRow,
				})
			})
			return nil
		},
	})

	return nil
}

// Returns the primary key values of the impacted rows by looking up the
// metadata in the passed db.
func (c *SQLiteWatchChange) ExtractKeys(db *sql.DB) ([][]interface{}, error) {
	// guard this edge just in case!
	if c.DatabaseName != "main" {
		return nil, errors.New("Only meant to be used on main database")
	}

	// go straight for the sqlite db info instead of going through the dbinfo
	// package, which lets us avoid importing a mysql dependency.
	dbInfo := SqliteDBInfo{Db: db}
	colInfos, err := dbInfo.GetColumnInfo(context.Background(), []string{c.TableName})
	if err != nil {
		return nil, err
	}

	exKey := func(row []interface{}) ([]interface{}, error) {
		key := []interface{}{}
		for _, colInfo := range colInfos {
			if colInfo.IsPrimaryKey {
				if colInfo.Index >= len(row) {
					// Should never happen, but yeah.
					return nil, errors.New("column info couldn't be matched to row")
				}
				// use a placeholder to scan the value of the column. it will use the
				// column metadata to correctly convert byte slices into strings
				// where appropriate.
				ph := scanfunc.Placeholder{
					Col: schema.DBColumnMeta{
						Name: colInfo.ColumnName,
						Type: colInfo.DataType,
					},
				}
				if err := ph.Scan(row[colInfo.Index]); err != nil {
					return nil, fmt.Errorf("scan key value column: %w", err)
				}
				key = append(key, pkAndMeta{
					Name:  colInfo.ColumnName,
					Type:  colInfo.DataType,
					Value: ph.Val,
				})
			}
		}
		return key, nil
	}

	keys := [][]interface{}{}
	for _, row := range [][]interface{}{c.OldRow, c.NewRow} {
		if row != nil {
			key, err := exKey(row)
			if err != nil {
				return nil, err
			}
			if len(key) > 0 {
				keys = append(keys, key)
			}
		}
	}
	return keys, nil
}
