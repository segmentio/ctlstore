package ldbwriter

import (
	"context"
	"sync/atomic"

	"github.com/segmentio/ctlstore/pkg/changelog"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/events/v2"
)

type ChangelogCallback struct {
	ChangelogWriter *changelog.ChangelogWriter
	Seq             int64
}

func (c *ChangelogCallback) LDBWritten(ctx context.Context, data LDBWriteMetadata) {
	for _, change := range data.Changes {
		fam, tbl, err := schema.DecodeLDBTableName(change.TableName)
		if err != nil {
			// This is expected because it'll capture tables like ctlstore_dml_ledger,
			// which aren't tables this cares about.
			events.Debug("Skipped logging change to %{tableName}s, can't decode table: %{error}v",
				change.TableName,
				err)
			continue
		}

		keys, err := change.ExtractKeys(data.DB)
		if err != nil {
			events.Log("Skipped logging change to %{tableName}, can't extract keys: %{error}v",
				change.TableName,
				err)
			continue
		}

		for _, key := range keys {
			seq := atomic.AddInt64(&c.Seq, 1)
			err = c.ChangelogWriter.WriteChange(changelog.ChangelogEntry{
				Seq:       seq,
				Op:        changelog.MapSQLiteOpToChangeOp(change.Op),
				LedgerSeq: data.Statement.Sequence,
				Family:    fam.Name,
				Table:     tbl.Name,
				Key:       key,
			})
			if err != nil {
				events.Log("Skipped logging change to %{family}s.%{table}s:%{key}v: %{err}v",
					fam, tbl, key, err)
				continue
			}
		}
	}
}
