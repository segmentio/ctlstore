package ldbwriter

import (
	"context"
	"database/sql"
	"sync/atomic"

	"github.com/segmentio/ctlstore/pkg/changelog"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlite"
	"github.com/segmentio/log"
)

type LDBWriterWithChangelog struct {
	LdbWriter       LDBWriter
	ChangelogWriter *changelog.ChangelogWriter
	DB              *sql.DB
	ChangeBuffer    *sqlite.SQLChangeBuffer
	Seq             int64
}

// NOTE: How does the changelog work?
//
// This is sort of the crux of how the changelog comes together. The Reflector
// sets a pre-update hook which populates a channel with any changes that happen
// in the LDB. These changes end up on a buffered channel. After each statement
// is executed, the pre-update hook will get called, filling in the channel. Once
// that ApplyDMLStatement returns, the DML statement is committed and the channel
// contains the contents of the update. Then this function takes over, extracts
// the keys from the update, and writes them to the changelogWriter.
//
// This is pretty complex, but after enumerating about 8 different options, it
// ended up actually being the most simple. Other options involved not-so-great
// options like parsing SQL or maintaining triggers on every table.
func (w *LDBWriterWithChangelog) ApplyDMLStatement(ctx context.Context, statement schema.DMLStatement) error {
	err := w.LdbWriter.ApplyDMLStatement(ctx, statement)
	if err != nil {
		return err
	}

	for _, change := range w.ChangeBuffer.Pop() {
		fam, tbl, err := schema.DecodeLDBTableName(change.TableName)
		if err != nil {
			// This is expected because it'll capture tables like ctlstore_dml_ledger,
			// which aren't tables this cares about.
			log.EventDebug("Skipped logging change to %{tableName}s, can't decode table: %{error}v",
				change.TableName,
				err)
			continue
		}

		keys, err := change.ExtractKeys(w.DB)
		if err != nil {
			log.EventLog("Skipped logging change to %{tableName}, can't extract keys: %{error}v",
				change.TableName,
				err)
			continue
		}

		for _, key := range keys {
			seq := atomic.AddInt64(&w.Seq, 1)
			err = w.ChangelogWriter.WriteChange(changelog.ChangelogEntry{
				Seq:    seq,
				Family: fam.Name,
				Table:  tbl.Name,
				Key:    key,
			})
			if err != nil {
				log.EventLog("Skipped logging change to %{family}s.%{table}s:%{key}v: %{err}v",
					fam, tbl, key, err)
				continue
			}
		}
	}
	return nil
}
