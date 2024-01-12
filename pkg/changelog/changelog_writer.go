package changelog

import (
	"encoding/json"
	"github.com/segmentio/ctlstore/pkg/sqlite"

	"github.com/pkg/errors"
	"github.com/segmentio/events/v2"
)

type (
	// WriteLine writes a line to something
	WriteLine interface {
		WriteLine(string) error
	}
	ChangelogWriter struct {
		WriteLine WriteLine
	}
	ChangelogEntry struct {
		Seq    int64
		Type   sqlite.ChangeType
		Family string
		Table  string
		Key    []interface{}
	}
)

func NewChangelogEntry(seq int64, family string, table string, key []interface{}) *ChangelogEntry {
	return &ChangelogEntry{Seq: seq, Family: family, Table: table, Key: key}
}

func (w *ChangelogWriter) WriteChange(e ChangelogEntry) error {
	structure := struct {
		Seq    int64         `json:"seq"`
		Type   string        `json:"type"`
		Family string        `json:"family"`
		Table  string        `json:"table"`
		Key    []interface{} `json:"key"`
	}{
		e.Seq,
		e.Type.String(),
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
