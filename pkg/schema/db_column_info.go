package schema

type DBColumnInfo struct {
	TableName    string
	Index        int
	ColumnName   string
	DataType     string
	IsPrimaryKey bool
}
