package reflector

import (
	"context"
	"os"
	"time"

	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"

	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/ldbwriter"
	"github.com/segmentio/ctlstore/pkg/utils"
)

type (
	MonitorConfig struct {
		PollInterval time.Duration
		Path         string
	}

	// WALMonitor is responsible for querying the file size of sqlite's WAL file while in WAL mode as well as sqlite's checkpointing of the WAL file.
	WALMonitor struct {
		// walPath file system location the sqlite wal file is located
		walPath     string
		walSizeFunc walSizeFunc
		// tickerFunc returns a ticker configured for the polling interval
		tickerFunc   func() *time.Ticker
		cpTesterFunc checkpointTesterFunc
		// consecutiveMaxErrors indicates when to stop performing a monitor when it fails consecutiveMaxErrors in a row
		// under default configuration, this is 5 minutes of failures before stopping
		consecutiveMaxErrors int
	}
	// returns the size of the wal file, or error
	walSizeFunc func(string) (int64, error)
	// returns WAL checkpoint status, or error
	checkpointTesterFunc func() (*ldbwriter.PragmaWALResult, error)
	// MonitorOps configuration functions that customize the monitor
	MonitorOps func(config *WALMonitor)
)

func NewMonitor(cfg MonitorConfig, checkpointTester checkpointTesterFunc, opts ...MonitorOps) *WALMonitor {
	m := &WALMonitor{
		walPath:              cfg.Path,
		cpTesterFunc:         checkpointTester,
		consecutiveMaxErrors: 5,
		tickerFunc: func() *time.Ticker {
			return time.NewTicker(cfg.PollInterval)
		},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m
}

// Start runs the wal file size check and sqlite checkpoint check on PollInterval's cadence
// this method blocks
func (m *WALMonitor) Start(ctx context.Context) {
	events.Log("WAL monitor starting")
	defer events.Log("WAL monitor stopped")
	if m.walPath == "" {
		events.Log("Not monitoring the WAL because its path is not set")
		return
	}
	sizeCtx, sizeCancel := context.WithCancel(ctx)
	statsFailedInARow := 0
	fn := m.getWALSize
	if m.walSizeFunc != nil {
		fn = m.walSizeFunc
	}
	go utils.CtxFireLoopTicker(sizeCtx, m.tickerFunc(), func() {
		// possible for ticker to invoke another loop before cancel makes it to the Done channel
		if statsFailedInARow >= m.consecutiveMaxErrors {
			return
		}
		size, err := fn(m.walPath)
		if err != nil {
			events.Log("error retrieving wal stat, %s", err)
			statsFailedInARow++
			if statsFailedInARow >= m.consecutiveMaxErrors {
				// cancel to prevent log spamming
				events.Log("canceling WAL size monitoring due to consistent error, %s", err)
				errs.Incr("reflector.wal_monitor.persistent_stat_error")
				sizeCancel()
			}
			return
		}
		stats.Set("wal-file-size", size)
		statsFailedInARow = 0
	})

	cpFailedInARow := 0
	cpCtx, cpCancel := context.WithCancel(ctx)
	utils.CtxFireLoopTicker(cpCtx, m.tickerFunc(), func() {
		// possible for ticker to invoke another loop before cancel makes it to the Done channel
		if cpFailedInARow >= m.consecutiveMaxErrors {
			return
		}
		res, err := m.cpTesterFunc()
		if err != nil {
			events.Log("error checking wal's checkpoint status, %s", err)
			cpFailedInARow++
			if cpFailedInARow >= m.consecutiveMaxErrors {
				// cancel to prevent log spamming
				events.Log("canceling WAL checkpoint monitoring due to consistent error, %s", err)
				errs.Incr("reflector.wal_monitor.persistent_checkpoint_error")
				cpCancel()
			}
			return
		}
		isBusy := "false"
		if res.Busy == 1 {
			isBusy = "true"
		}
		stats.Set("wal-checkpoint-status", 1, stats.T("busy", isBusy))

		if res.Log-res.Checkpointed > 0 {
			stats.Set("wal-uncommitted-pages", res.Log-res.Checkpointed)
		}
		cpFailedInARow = 0
	})
}

// getWALSize default implementation of walSizeFunc
func (m *WALMonitor) getWALSize(path string) (int64, error) {
	s, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return s.Size(), nil
}
