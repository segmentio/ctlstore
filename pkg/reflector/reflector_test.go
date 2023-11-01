package reflector

import (
	"context"
	"database/sql"
	"encoding/base64"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/segmentio/ctlstore/pkg/ctldb"
	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/ldbwriter"
	"github.com/segmentio/ctlstore/pkg/ledger"
	"github.com/segmentio/events/v2"
	"github.com/stretchr/testify/require"
)

func TestShovelSequenceReset(t *testing.T) {
	tmpPath, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tmpPath)

	upstreamDbPath := filepath.Join(tmpPath, "upstream.db")
	ldbDbPath := filepath.Join(tmpPath, "ldb.db")
	changelogPath := filepath.Join(tmpPath, "changelog")
	emptyLdbPath := filepath.Join(tmpPath, "emptyLdb.db")

	upstreamSQL := ctldb.CtlDBSchemaByDriver["sqlite3"]
	upstreamDB, err := sql.Open("sqlite3", upstreamDbPath)
	require.NoError(t, err)
	_, err = upstreamDB.Exec(upstreamSQL)
	require.NoError(t, err)

	ldbDB, err := sql.Open("sqlite3", emptyLdbPath)
	require.NoError(t, err)

	defer ldbDB.Close()
	ldb.EnsureLdbInitialized(context.TODO(), ldbDB)

	emptyLdbContents, err := ioutil.ReadFile(emptyLdbPath)
	require.NoError(t, err)

	encodedEmpty := base64.URLEncoding.EncodeToString(emptyLdbContents)
	dataURI := "data:" + encodedEmpty

	cfg := ReflectorConfig{
		LDBPath:       ldbDbPath,
		BootstrapURL:  dataURI,
		ChangelogPath: changelogPath,
		ChangelogSize: 1 * 1024 * 1024,
		Upstream: UpstreamConfig{
			Driver:         "sqlite3",
			DSN:            upstreamDbPath,
			LedgerTable:    "ctlstore_dml_ledger",
			QueryBlockSize: 1,
			PollInterval:   10 * time.Millisecond,
			PollTimeout:    10 * time.Millisecond,
		},
		LedgerHealth: ledger.HealthConfig{
			DisableECSBehavior: true,
		},
	}
	reflector, err := ReflectorFromConfig(cfg)
	require.NoError(t, err)
	require.NotNil(t, reflector)

	getCallback := func(shovel *shovel) *ldbwriter.ChangelogCallback {
		writer := shovel.writer
		w, ok := writer.(*ldbwriter.CallbackWriter)
		if !ok {
			t.Fatalf("expected callback writer but got %T", writer)
		}
		for _, callback := range w.Callbacks {
			c, ok := callback.(*ldbwriter.ChangelogCallback)
			if !ok {
				continue
			}
			return c
		}
		t.Fatal("No changelog callback found :(")
		return nil
	}

	getSeq := func(shovel *shovel) int64 {
		cb := getCallback(shovel)
		return atomic.LoadInt64(&cb.Seq)
	}

	storeSeq := func(shovel *shovel, v int64) {
		cb := getCallback(shovel)
		atomic.StoreInt64(&cb.Seq, v)
	}

	shovelFunc := reflector.shovel
	shovel, err := shovelFunc()
	require.NoError(t, err)
	require.EqualValues(t, 0, getSeq(shovel))

	// artificially set the sequence number
	storeSeq(shovel, 42)
	require.EqualValues(t, 42, getSeq(shovel))

	// recreate the shovel using the shovelFunc and make sure the sequence
	// has been reset.
	shovel, err = shovelFunc()
	require.NoError(t, err)
	require.EqualValues(t, 0, getSeq(shovel))
}

func TestReflector(t *testing.T) {
	tmpPath, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("Encountered unexpected error creating temp path, %v", err)
	}
	defer os.RemoveAll(tmpPath)

	upstreamDbPath := filepath.Join(tmpPath, "upstream.db")
	ldbDbPath := filepath.Join(tmpPath, "ldb.db")
	changelogPath := filepath.Join(tmpPath, "changelog")

	emptyLdbPath := filepath.Join(tmpPath, "emptyLdb.db")
	ldbDB, err := sql.Open("sqlite3", emptyLdbPath)
	require.NoError(t, err)

	defer ldbDB.Close()
	ldb.EnsureLdbInitialized(context.TODO(), ldbDB)

	emptyLdbContents, err := ioutil.ReadFile(emptyLdbPath)
	require.NoError(t, err)

	encodedEmpty := base64.URLEncoding.EncodeToString(emptyLdbContents)
	dataURI := "data:" + encodedEmpty

	cfg := ReflectorConfig{
		LDBPath:       ldbDbPath,
		BootstrapURL:  dataURI,
		ChangelogPath: changelogPath,
		ChangelogSize: 1 * 1024 * 1024,
		Upstream: UpstreamConfig{
			Driver:         "sqlite3",
			DSN:            upstreamDbPath,
			LedgerTable:    "ctlstore_dml_ledger",
			QueryBlockSize: 1,
			PollInterval:   10 * time.Millisecond,
			PollTimeout:    10 * time.Millisecond,
		},
		LedgerHealth: ledger.HealthConfig{
			DisableECSBehavior: true,
			PollInterval:       10 * time.Second,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())

	upstreamDb, err := sql.Open("sqlite3", upstreamDbPath)
	require.NoError(t, err)
	defer upstreamDb.Close()

	_, err = upstreamDb.Exec(`
		CREATE TABLE ctlstore_dml_ledger (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			leader_ts INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,
			statement VARCHAR(786432)
		);
	`)
	require.NoError(t, err)

	ledgerStmts := []string{
		`CREATE TABLE family1___table1234 (
			field1 INTEGER PRIMARY KEY,
			field2 VARCHAR
		);`,
		`INSERT INTO family1___table1234 VALUES(1234, 'hello');`,
	}

	for _, stmt := range ledgerStmts {
		_, err := upstreamDb.Exec("INSERT INTO ctlstore_dml_ledger (statement) VALUES(?)", stmt)
		require.NoError(t, err)
	}

	reflector, err := ReflectorFromConfig(cfg)
	require.NoError(t, err)

	isTerminated := int64(0)

	waitCh := make(chan struct{})
	go func() {
		close(waitCh)
		reflector.Start(ctx)
		events.Log("Reflector terminated")
		atomic.AddInt64(&isTerminated, 1)
	}()
	<-waitCh

	select {
	case <-time.After(100 * time.Millisecond):
		cancel()
	}

	clBytes, err := ioutil.ReadFile(changelogPath)
	require.NoError(t, err)

	expectChangelog := "{\"seq\":1,\"family\":\"family1\",\"table\":\"table1234\",\"key\":[{\"name\":\"field1\",\"type\":\"INTEGER\",\"value\":1234}]}\n"
	if diff := cmp.Diff(expectChangelog, string(clBytes)); diff != "" {
		t.Errorf("Changelog contents differ\n%s", diff)
	}

	select {
	case <-time.After(100 * time.Millisecond):
		isTerm := atomic.LoadInt64(&isTerminated)
		if isTerm == 0 {
			t.Errorf("Expected Reflector instance to cancel.")
		}
	}

	err = reflector.Close()
	require.NoError(t, err)
}

func TestEmitMetricFromFile(t *testing.T) {
	for _, tt := range []struct {
		name     string
		fileName string
		extra    string
		content  string
		perm     int
		err      error
	}{
		{
			"file does not exist doesn't return error",
			"1.jso",
			"n",
			"{\"startTime\": \"6\", \"downloaded\": \"true\", \"compressed\": \"false\"}",
			0664,
			nil,
		},
		{
			"file exist but unable to open",
			"2.json",
			"",
			"{\"startTime\": 6, \"downloaded\": \"true\", \"compressed\": \"false\"}",
			064,
			errors.New("permission denied"),
		},
		{
			"invalid character",
			"3.json",
			"",
			"{\"startTime\": \"6, \"downloaded\": \"true\", \"compressed\": \"false\"}",
			0664,
			errors.New("invalid character"),
		},
		{
			"invalid key",
			"4.json",
			"",
			"{\"start\": \"6\", \"downloaded\": \"true\", \"compressed\": \"false\"}",
			0664,
			errors.New("unknown field"),
		},
		{
			"valid content",
			"5.json",
			"",
			"{\"startTime\": 6, \"downloaded\": \"true\", \"compressed\": \"false\"}",
			0664,
			nil,
		},
	} {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tempdir := t.TempDir()
			f, err := os.CreateTemp(tempdir, test.fileName)
			assert.NoError(t, err)

			_, err = f.Write([]byte(test.content))
			assert.NoError(t, err)

			err = os.Chmod(f.Name(), os.FileMode(test.perm))
			assert.NoError(t, err)

			err = emitMetricFromFile(f.Name() + test.extra)

			if test.err == nil {
				require.NoError(t, err)
			} else {
				require.Contains(t, err.Error(), test.err.Error())
			}
		})
	}
}
