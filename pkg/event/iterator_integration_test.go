package event

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	changelogpkg "github.com/segmentio/ctlstore/pkg/changelog"
	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/ldbwriter"
	"github.com/segmentio/ctlstore/pkg/logwriter"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlite"
	"github.com/segmentio/ctlstore/pkg/tests"
)

func TestIteratorIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	f, teardown := tests.WithTmpFile(t, "changelog")
	defer teardown()

	changeBuffer := new(sqlite.SQLChangeBuffer)
	driverName := fmt.Sprintf("%s_%d", ldb.LDBDatabaseDriver, time.Now().UnixNano())

	ldbTmpPath, teardown := ldb.NewLDBTmpPath(t)
	defer teardown()

	dsn := fmt.Sprintf("file:%s", ldbTmpPath)

	err := sqlite.RegisterSQLiteWatch(driverName, changeBuffer)
	require.NoError(t, err)

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		t.Fatalf("Couldn't open SQLite db, error %v", err)
	}
	err = ldb.EnsureLdbInitialized(context.Background(), db)
	if err != nil {
		t.Fatalf("Couldn't initialize SQLite db, error %v", err)
	}

	sqlWriter := &ldbwriter.SqlLdbWriter{Db: db}
	sizedLogWriter := &logwriter.SizedLogWriter{Path: f.Name(), FileMode: 0644, RotateSize: 1024 * 1024}
	changeLogWriter := &changelogpkg.ChangelogWriter{WriteLine: sizedLogWriter}
	writer := &ldbwriter.LDBWriterWithChangelog{
		LdbWriter:       sqlWriter,
		ChangelogWriter: changeLogWriter,
		DB:              db,
		ChangeBuffer:    changeBuffer,
	}

	const numChanges = 50

	go func() {
		err := writer.ApplyDMLStatement(ctx, schema.NewTestDMLStatement("CREATE TABLE fam___foo (id int primary key not null, val VARCHAR);"))
		require.NoError(t, err)

		for i := 0; i < numChanges; i++ {
			err = writer.ApplyDMLStatement(ctx, schema.NewTestDMLStatement(fmt.Sprintf("INSERT INTO fam___foo VALUES(%d, 'hello');", i)))
			require.NoError(t, err)
		}
	}()

	iter, err := NewIterator(ctx, f.Name())
	require.NoError(t, err)
	require.NotNil(t, iter)

	for i := 0; i < numChanges; i++ {
		e, err := iter.Next(ctx)
		require.NoError(t, err)
		require.EqualValues(t, i+1, e.Sequence)
		update := e.RowUpdate
		require.Equal(t, "fam", update.FamilyName)
		require.Equal(t, "foo", update.TableName)
		keys := update.Keys
		require.EqualValues(t, []Key{{
			Name:  "id",
			Type:  "INT",
			Value: float64(i),
		}}, keys)

	}

}
