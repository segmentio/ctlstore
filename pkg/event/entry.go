package event

// entry represents a single row in the changelog
// e.g.
//
//	{"seq":1,"ledgerSeq":42,"tx":false,"family":"fam","table":"foo","key":[{"name":"id","type":"int","value":1}]}
type entry struct {
	Seq         int64  `json:"seq"`
	LedgerSeq   int64  `json:"ledgerSeq"`
	Transaction bool   `json:"tx"`
	Family      string `json:"family"`
	Table       string `json:"table"`
	Key         []Key  `json:"key"`
}

// event converts the entry into an event for the iterator to return
func (e entry) event() Event {
	return Event{
		Sequence:       e.Seq,
		LedgerSequence: e.LedgerSeq,
		Transaction:    e.Transaction,
		RowUpdate: RowUpdate{
			FamilyName: e.Family,
			TableName:  e.Table,
			Keys:       e.Key,
		},
	}
}
