package changelog

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/go-sqlite3"
)

// ChangeOp is the type of operation that was done to the DB
type ChangeOp int

const (
	INSERT_OP ChangeOp = iota
	UPDATE_OP
	DELETE_OP
	UNKNOWN_OP
	// Add other statement types here
)

// Map SQLite's update operation types to our own internal type
func MapSQLiteOpToChangeOp(op int) ChangeOp {
	switch op {
	case sqlite3.SQLITE_INSERT:
		return INSERT_OP
	case sqlite3.SQLITE_UPDATE:
		return UPDATE_OP
	case sqlite3.SQLITE_DELETE:
		return DELETE_OP
	default:
		return UNKNOWN_OP
	}
}

func (c ChangeOp) String() string {
	switch c {
	case INSERT_OP:
		return "insert"
	case UPDATE_OP:
		return "update"
	case DELETE_OP:
		return "delete"
	default:
		return "unknown"
	}
}

type (
	// WriteLine writes a line to something
	WriteLine interface {
		WriteLine(string) error
	}
	ChangelogWriter struct {
		WriteLine WriteLine
	}
	ChangelogEntry struct {
		Seq         int64
		Op          ChangeOp
		Family      string
		Table       string
		Key         []interface{}
		LedgerSeq   schema.DMLSequence
		Transaction bool
	}
)

func NewChangelogEntry(seq int64, family string, table string, key []interface{}) *ChangelogEntry {
	return &ChangelogEntry{Seq: seq, Family: family, Table: table, Key: key}
}

func (w *ChangelogWriter) WriteChange(e ChangelogEntry) error {
	structure := struct {
		Seq         int64         `json:"seq"`
		LedgerSeq   int64         `json:"ledgerSeq"`
		Transaction bool          `json:"tx"`
		Op          string        `json:"op"`
		Family      string        `json:"family"`
		Table       string        `json:"table"`
		Key         []interface{} `json:"key"`
	}{
		e.Seq,
		e.LedgerSeq.Int(),
		e.Transaction,
		e.Op.String(),
		e.Family,
		e.Table,
		e.Key,
	}

	bytes, err := json.Marshal(structure)
	if err != nil {
		return errors.Wrap(err, "error marshalling json")
	}

	events.Debug("changelogWriter.WriteChange: %{family}s.%{table}s => %{key}v",
		e.Family, e.Table, e.Key)

	return w.WriteLine.WriteLine(string(bytes))
}
