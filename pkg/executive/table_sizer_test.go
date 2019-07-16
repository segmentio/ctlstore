package executive

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/units"
	"github.com/stretchr/testify/require"
)

func TestTableSizerMySQL(t *testing.T) {
	doTestTableSizer(t, "mysql")
}

func TestTableSizerSqlite3(t *testing.T) {
	doTestTableSizer(t, "sqlite3")
}

func doTestTableSizer(t *testing.T, dbType string) {
	sqlite3 := dbType == "sqlite3"
	defaultLimit := limits.SizeLimits{
		MaxSize:  1 * units.MEGABYTE,
		WarnSize: 500 * units.KILOBYTE,
	}
	db, teardown := newCtlDBTestConnection(t, dbType)
	defer teardown()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sizer := newTableSizer(db, dbType, defaultLimit, 10*time.Millisecond)
	found, err := sizer.tableOK(schema.FamilyTable{Family: "foo", Table: "bar"})
	require.False(t, found)
	require.NoError(t, err)

	// create the table that we'll be using. it's important for it to look
	// like a normal table (family+tableName) because the limiter only
	// checks tables that are named like that.
	_, err = db.ExecContext(ctx, `
		create table foo___bar (
			name varchar(100) NOT NULL PRIMARY KEY,
			data mediumblob
		);
	`)
	require.NoError(t, err)

	// helper method
	verifyFound := func(found bool) {
		if sqlite3 {
			// sqlite3 doesn't support table sizing so it will not find
			// any tables in its cache that are being checked
			require.False(t, found)
		} else {
			require.True(t, found)
		}
	}

	// do a refresh and verify that the table was found
	require.NoError(t, sizer.refresh(ctx))
	found, err = sizer.tableOK(schema.FamilyTable{Family: "foo", Table: "bar"})
	verifyFound(found)
	require.NoError(t, err)

	var rowIdx int64
	insertData := func(numBytes int64) error {
		rowIdx++
		data := make([]byte, numBytes)
		rand.Read(data)
		_, err = db.ExecContext(ctx, "insert into foo___bar (name, data) values(?,?)",
			fmt.Sprintf("row-%d", rowIdx), data)
		return err
	}

	// insert 500K of data. should still be ok
	require.NoError(t, insertData(500*units.KILOBYTE))
	require.NoError(t, sizer.refresh(ctx))
	found, err = sizer.tableOK(schema.FamilyTable{Family: "foo", Table: "bar"})
	verifyFound(found)
	require.NoError(t, err)

	// insert a couple megs of data. should fail
	require.NoError(t, insertData(units.MEGABYTE))
	require.NoError(t, insertData(units.MEGABYTE))
	require.NoError(t, sizer.refresh(ctx))
	found, err = sizer.tableOK(schema.FamilyTable{Family: "foo", Table: "bar"})
	verifyFound(found)
	if sqlite3 {
		// when using sqlite3 we do not reject writes based on table sizes
		require.Nil(t, err)
	} else {
		require.NotNil(t, err)
		require.EqualValues(t, "table 'foo___bar' has exceeded the max size of 1048576", err.Error())
	}

	// add a table override for this particular table to allow it to grow more.
	_, err = db.ExecContext(ctx, "insert into max_table_sizes "+
		"(family_name, table_name, warn_size_bytes, max_size_bytes) values "+
		"(?,?,?,?)", "foo", "bar", 1*units.MEGABYTE, 100*units.MEGABYTE)
	require.NoError(t, err)
	// force the sizer to reload table sizes
	require.NoError(t, sizer.refresh(ctx))

	// ensure that the table size can grow more now
	found, err = sizer.tableOK(schema.FamilyTable{Family: "foo", Table: "bar"})
	verifyFound(found)
	require.NoError(t, err)

	// add a table override for this table, but make it lower than the default
	_, err = db.ExecContext(ctx, "replace into max_table_sizes "+
		"(family_name, table_name, warn_size_bytes, max_size_bytes) values "+
		"(?,?,?,?)", "foo", "bar", 1, 1)
	require.NoError(t, err)
	// force the sizer to reload table sizes
	require.NoError(t, sizer.refresh(ctx))

	// ensure that the table no longer allows writes
	found, err = sizer.tableOK(schema.FamilyTable{Family: "foo", Table: "bar"})
	if sqlite3 {
		// when using sqlite3 we do not reject writes based on table sizes
		require.Nil(t, err)
	} else {
		require.NotNil(t, err)
		require.EqualValues(t, "table 'foo___bar' has exceeded the max size of 1", err.Error())
	}
}
