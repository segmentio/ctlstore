package schema

import "database/sql"

type DBColumnMeta struct {
	Name string
	Type string
}

func DBColumnMetaFromRows(rows *sql.Rows) ([]DBColumnMeta, error) {
	typs, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	res := make([]DBColumnMeta, 0, len(typs))
	for _, typ := range typs {
		res = append(res, DBColumnMeta{
			Name: typ.Name(),
			Type: typ.DatabaseTypeName(),
		})
	}
	return res, nil
}
