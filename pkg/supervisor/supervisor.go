package supervisor

import (
	"context"
	"database/sql"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/reflector"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"
)

type Supervisor interface {
	Start(ctx context.Context)
	io.Closer
}

type Reflector interface {
	Start(ctx context.Context) error
	Close() error
}

type SupervisorConfig struct {
	SnapshotInterval time.Duration
	SnapshotURL      string
	LDBPath          string
	Reflector        Reflector
}

type supervisor struct {
	SleepDuration   time.Duration
	BreatheDuration time.Duration
	LDBPath         string
	Snapshots       []archivedSnapshot
	reflectorCtl    *reflector.ReflectorCtl
}

func SupervisorFromConfig(config SupervisorConfig) (Supervisor, error) {
	var snapshots []archivedSnapshot
	urls := strings.Split(config.SnapshotURL, ",")
	for _, url := range urls {
		snapshot, err := archivedSnapshotFromURL(url)
		if err != nil {
			return nil, errors.Wrapf(err, "configure snapshot for '%s'", url)
		}
		snapshots = append(snapshots, snapshot)
	}
	return &supervisor{
		SleepDuration:   config.SnapshotInterval,
		BreatheDuration: 5 * time.Second,
		LDBPath:         config.LDBPath,
		Snapshots:       snapshots,
		reflectorCtl:    reflector.NewReflectorCtl(config.Reflector),
	}, nil
}

func (s *supervisor) snapshot(ctx context.Context) error {
	events.Log("Taking a snapshot")
	s.reflectorCtl.Stop(ctx)
	defer s.reflectorCtl.Start(ctx)
	if err := s.checkpointLDB(); err != nil {
		return errors.Wrap(err, "checkpoint ldb")
	}
	info, err := os.Stat(s.LDBPath)
	if err != nil {
		return errors.Wrap(err, "stat ldb path")
	}
	stats.Set("ldb-size-bytes", info.Size())
	errs := make(chan error, len(s.Snapshots))
	for _, snapshot := range s.Snapshots {
		go func(snapshot archivedSnapshot) {
			err := snapshot.Upload(ctx, s.LDBPath)
			errs <- errors.Wrapf(err, "upload snapshot")
		}(snapshot)
	}
	for range s.Snapshots {
		if err := <-errs; err != nil {
			return err
		}
	}
	return nil
}

func (s *supervisor) checkpointLDB() error {
	ctx := context.Background() // we do not want to interrupt this operation
	srcDb, err := sql.Open("sqlite3", s.LDBPath+"?_journal_mode=wal")
	if err != nil {
		return errors.Wrap(err, "opening source db")
	}
	defer srcDb.Close()
	conn, err := srcDb.Conn(ctx)
	if err != nil {
		return errors.Wrap(err, "src db connection")
	}
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE);")
	if err != nil {
		return errors.Wrap(err, "checkpointing database")
	}
	_, err = conn.ExecContext(ctx, "VACUUM")
	if err != nil {
		return errors.Wrap(err, "vacuuming database")
	}
	// This will prevent any writes while the copy is taking place
	_, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE TRANSACTION;")
	if err != nil {
		return errors.Wrap(err, "locking database")
	}
	events.Log("Acquired write lock on %{srcDb}s", s.LDBPath)
	_, err = conn.ExecContext(ctx, "COMMIT;")
	if err != nil {
		return errors.Wrap(err, "commit")
	}
	events.Log("Released write lock on %{srcDb}s", s.LDBPath)
	events.Log("Checkpointed WAL on %{srcDb}s", s.LDBPath)
	return nil
}

func (s *supervisor) incrementSnapshotErrorMetric(value int) {
	stats.Add("snapshot-errors", value)
}

func (s *supervisor) Start(ctx context.Context) {
	s.incrementSnapshotErrorMetric(0) // initialize the metric since it's sparse
	events.Log("Starting supervisor")
	s.reflectorCtl.Start(ctx)
	defer events.Log("Stopped Supervisor")
	sleepDur := s.SleepDuration
	for {
		// Wait for the reflector to make changes to its LDB before stopping it.  Sometimes
		// we need more time than to process a big transaction, such as a large cleanup of
		// the LDB that does a deletion of many rows.
		//
		// ⚠️ If you don't increase the sleep duration enough to allow a transaction to complete,
		// then the supervisor will get stuck and never make progress.
		// Such a situation has a growing compound effect on the Ctlstore ecosystem,
		// as all new reflectors have old snapshots and thus need to pull down an ever-growing
		// number of ledger updates to sync-up with the latest state.
		// This also creates an ever-increasing load on the Ctlstore DB (ctldb).
		//
		// TODO:
		//   * Add detection of progress stopping (same ledger seq on 2+ runs) and backoff the sleep duration
		//     automatically, up to a limit.
		//       * Would be nice to be able to determine what the state of the LDB actions were
		//         before we stopped the reflector and did the snapshot, so we can log it and spit out
		//         a metric about it.
		//   * Ensure we have good metrics around these behaviors; e.g., number of ledger entries processed
		//     in the last iteration of the reflector.
		//
		select {
		case <-time.After(sleepDur):
			// reset to default
			sleepDur = s.SleepDuration
		case <-ctx.Done():
			events.Log("Supervisor exiting because context done (err=%v)", ctx.Err())
			// Outer context is done, aborting everything
			return
		}
		err := s.snapshot(ctx)
		if err != nil && errors.Cause(err) != context.Canceled {
			s.incrementSnapshotErrorMetric(1)
			events.Log("Error taking snapshot: %{error}+v", err)
			// Use a shorter sleep duration for faster retries
			sleepDur = s.BreatheDuration
		}
	}
}

func (s *supervisor) Close() error {
	return s.reflectorCtl.Close()
}
