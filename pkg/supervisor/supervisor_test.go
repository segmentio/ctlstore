package supervisor

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pkg/errors"
	ldbpkg "github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/reflector/fakes"
	"github.com/stretchr/testify/require"
)

// Helper for building a GetLedgerLatency func.
func mockGetLedgerLatency(duration time.Duration, err error) func(ctx context.Context) (time.Duration, error) {
	return func(ctx context.Context) (time.Duration, error) {
		return duration, err
	}
}

func TestSupervisorParsingSnapshotURL(t *testing.T) {
	urls := "s3://segment-ctlstore-snapshots-stage/snapshot.db.gz,s3://segment-ctlstore-snapshots-stage/snapshot.db"
	sup, err := SupervisorFromConfig(SupervisorConfig{
		SnapshotURL:      urls,
		GetLedgerLatency: mockGetLedgerLatency(time.Second, nil),
		MaxLedgerLatency: time.Minute,
	})
	require.NoError(t, err)
	supi, ok := sup.(*supervisor)
	require.True(t, ok)
	require.Len(t, supi.Snapshots, 2)
	s1, ok := supi.Snapshots[0].(*s3Snapshot)
	require.True(t, ok)
	require.Equal(t, "segment-ctlstore-snapshots-stage", s1.Bucket)
	require.Equal(t, "/snapshot.db.gz", s1.Key)
	s2, ok := supi.Snapshots[1].(*s3Snapshot)
	require.True(t, ok)
	require.Equal(t, "segment-ctlstore-snapshots-stage", s2.Bucket)
	require.Equal(t, "/snapshot.db", s2.Key)
}

func TestSupervisor(t *testing.T) {
	tmpPath, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tmpPath)

	ldbDbPath := filepath.Join(tmpPath, "ldb.db")
	archivePath := filepath.Join(tmpPath, "archive.db")

	expectedSeqNumber := 100

	reflector := fakes.NewFakeReflector()
	defer func() {
		// reflector should not be running
		require.False(t, reflector.Running.IsSet())
		// reflector should be closed
		require.True(t, reflector.Closed.IsSet())
	}()

	cfg := SupervisorConfig{
		SnapshotInterval: 100 * time.Millisecond,
		SnapshotURL:      "file://" + archivePath,
		LDBPath:          ldbDbPath,
		Reflector:        reflector,
		GetLedgerLatency: mockGetLedgerLatency(time.Second, nil),
		MaxLedgerLatency: time.Minute,
	}

	sv, err := SupervisorFromConfig(cfg)
	require.NoError(t, err)
	defer sv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ldb, err := sql.Open("sqlite3", ldbDbPath+"?_journal_mode=wal&cache=shared")
	require.NoError(t, err)

	err = ldbpkg.EnsureLdbInitialized(ctx, ldb)
	require.NoError(t, err)

	_, err = ldb.Exec(
		fmt.Sprintf("REPLACE INTO %s (id, seq) VALUES(?, ?)", ldbpkg.LDBSeqTableName),
		ldbpkg.LDBSeqTableID, expectedSeqNumber)
	require.NoError(t, err)

	sctx, scancel := context.WithTimeout(ctx, 1*time.Second)
	defer scancel()

	fatalCh := make(chan string, 10)
	lockedCh := make(chan struct{})
	stoppedCh := make(chan struct{})

	fatality := func(msg string, args ...interface{}) {
		fmtd := fmt.Sprintf(msg, args...)
		fatalCh <- fmtd
		cancel()
	}

	handleFatalities := func() {
		select {
		case fatalMsg := <-fatalCh:
			t.Fatal(fatalMsg)
		default:
			return
		}
	}

	go func() {
		defer close(lockedCh)
		defer close(stoppedCh)

		conn, err := ldb.Conn(ctx)
		if err != nil {
			fatality("Unexpected error: %+v", err)
			return
		}
		defer conn.Close()

		_, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE TRANSACTION;")
		if err != nil {
			fatality("Unexpected error: %+v", err)
			return
		}

		// wait for the main goroutine to sync up
		lockedCh <- struct{}{}

		// Wait for supervisor to start
		time.Sleep(10 * time.Millisecond)

		_, err = conn.ExecContext(ctx, "COMMIT;")
		if err != nil {
			fatality("Unexpected error: %+v", err)
			return
		}

		// Wait for snapshot to complete
		time.Sleep(10 * time.Millisecond)

		// Cancels the context passed to the supervisor, which should cause it
		// to return from the Start() call
		scancel()
		<-stoppedCh
	}()
	handleFatalities()

	<-lockedCh
	handleFatalities()

	sv.Start(sctx)
	handleFatalities()

	stoppedCh <- struct{}{}
	handleFatalities()

	archDb, err := sql.Open("sqlite3", archivePath)
	require.NoError(t, err)

	row := archDb.QueryRow(
		fmt.Sprintf("SELECT seq FROM %s WHERE id = ?", ldbpkg.LDBSeqTableName),
		ldbpkg.LDBSeqTableID)

	var gotSeq int
	err = row.Scan(&gotSeq)
	require.NoError(t, err)
	require.EqualValues(t, expectedSeqNumber, gotSeq)
}

// verifies that the embedded reflector is properly shutdown
// and restarted before and after a snapshot is taken.
func TestSupervisorSnapshotReflectorCtl(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tmpPath, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tmpPath)

	ldbDbPath := filepath.Join(tmpPath, "ldb.db")
	archivePath := filepath.Join(tmpPath, "archive.db")

	ldb, err := sql.Open("sqlite3", ldbDbPath+"?_journal_mode=wal&cache=shared")
	require.NoError(t, err)
	err = ldbpkg.EnsureLdbInitialized(ctx, ldb)
	require.NoError(t, err)

	reflector := fakes.NewFakeReflector()
	supervisorI, err := SupervisorFromConfig(SupervisorConfig{
		SnapshotInterval: time.Hour, // we will manually invoke snapshot
		SnapshotURL:      "file://" + archivePath,
		LDBPath:          ldbDbPath,
		Reflector:        reflector,
		GetLedgerLatency: mockGetLedgerLatency(time.Second, nil),
		MaxLedgerLatency: time.Minute,
	})
	require.NoError(t, err)
	require.NotNil(t, supervisorI)

	supervisor := supervisorI.(*supervisor)
	supervisor.reflectorCtl.Start(ctx)
	require.Equal(t, "started", reflector.NextEvent(ctx))

	err = supervisor.snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, "stopped", reflector.NextEvent(ctx))
	require.Equal(t, "started", reflector.NextEvent(ctx))

	// verify no more events (steady state)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 0, len(reflector.Events))

	// do another snapshot and verify the reflector was stopped and then
	// started again.
	err = supervisor.snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, "stopped", reflector.NextEvent(ctx))
	require.Equal(t, "started", reflector.NextEvent(ctx))

	// verify no more events (steady state)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 0, len(reflector.Events))
}

func TestSupervisorMaximumLedgerLatency(t *testing.T) {
	tmpPath, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tmpPath)
	ldbDbPath := filepath.Join(tmpPath, "ldb.db")
	archivePath := filepath.Join(tmpPath, "archive.db")

	svI, err := SupervisorFromConfig(SupervisorConfig{
		SnapshotInterval: 100 * time.Millisecond,
		SnapshotURL:      "file://" + archivePath,
		LDBPath:          ldbDbPath,
		Reflector:        fakes.NewFakeReflector(),
		GetLedgerLatency: mockGetLedgerLatency(time.Second, nil),
		MaxLedgerLatency: time.Minute,
	})
	require.NoError(t, err)
	sv := svI.(*supervisor)
	defer sv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sctx, scancel := context.WithTimeout(ctx, 1*time.Second)
	defer scancel()

	go func() {
		// Wait for snapshot to complete
		time.Sleep(10 * time.Millisecond)
		// Cancels the context passed to the supervisor, which should cause it
		// to return from the Start() call
		scancel()
	}()

	// This should create a snapshot because the latency is below the max. An archive file should be created.
	sv.Start(sctx)

	_, err = os.Stat(archivePath)
	require.NoError(t, err, "Expected an archive to be created")

	// Now clear the archive file so we can verify that the supervisor does not
	// create a snapshot if the ledger latency is too much.
	err = os.Remove(archivePath)
	require.NoError(t, err, "Failed to remove archive")

	// Bump up the latency
	sv.getLedgerLatency = mockGetLedgerLatency(time.Hour, nil)

	sctx, scancel = context.WithTimeout(ctx, 1*time.Second)
	defer scancel()
	go func() {
		// Wait for snapshot to complete
		time.Sleep(10 * time.Millisecond)
		// Cancels the context passed to the supervisor, which should cause it
		// to return from the Start() call
		scancel()
	}()

	// This should skip a snapshot because of the latency, we don't expect an archive to be created.
	sv.Start(sctx)

	_, err = os.Stat(archivePath)
	require.Error(t, err, "Did not expect an archive to be created, due to max ledger latency")

	// Return an error from GetLedgerLatency
	sv.getLedgerLatency = mockGetLedgerLatency(time.Minute, errors.New("Oops"))

	sctx, scancel = context.WithTimeout(ctx, 1*time.Second)
	defer scancel()
	go func() {
		// Wait for snapshot to complete
		time.Sleep(10 * time.Millisecond)
		// Cancels the context passed to the supervisor, which should cause it
		// to return from the Start() call
		scancel()
	}()

	// This should skip a snapshot because of the error, we don't expect an archive to be created.
	sv.Start(sctx)

	_, err = os.Stat(archivePath)
	require.Error(t, err, "Did not expect an archive to be created, due to max ledger latency")
}
