package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlgen"
)

type SqliteDBInfo struct {
	Db *sql.DB
}

func (m *SqliteDBInfo) GetColumnInfo(ctx context.Context, tableNames []string) ([]schema.DBColumnInfo, error) {
	if len(tableNames) == 0 {
		return []schema.DBColumnInfo{}, nil
	}
	columnInfos := []schema.DBColumnInfo{}
	for _, tableName := range tableNames {
		err := func() error {
			qTableName, err := sqlgen.SQLQuote(tableName)
			if err != nil {
				return err
			}

			qs := fmt.Sprintf(
				"SELECT cid, name, type, pk FROM pragma_table_info(%s) "+
					"ORDER BY cid ASC",
				qTableName)

			rows, err := m.Db.QueryContext(ctx, qs)
			if err != nil {
				return err
			}
			defer rows.Close()

			for rows.Next() {
				var colID int
				var colName string
				var dataType string
				var pk int

				err = rows.Scan(&colID, &colName, &dataType, &pk)
				if err != nil {
					return err
				}

				columnInfos = append(columnInfos, schema.DBColumnInfo{
					TableName:    tableName,
					Index:        colID,
					ColumnName:   colName,
					DataType:     dataType,
					IsPrimaryKey: (pk > 0),
				})
			}
			return rows.Err()
		}()
		if err != nil {
			return nil, err
		}
	}
	return columnInfos, nil
}
