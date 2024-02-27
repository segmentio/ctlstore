package event

// Event is the type that the Iterator produces
type Event struct {
	Sequence       int64
	LedgerSequence int64
	Transaction    bool
	RowUpdate      RowUpdate
}

// RowUpdate represents a single row update
type RowUpdate struct {
	FamilyName string `json:"family"`
	TableName  string `json:"table"`
	Keys       []Key  `json:"keys"`
}

// Key represents a single primary key column value and metadata
type Key struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}
