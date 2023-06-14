package reflector

import (
	"context"
	"os"
	"time"

	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"

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
		walPath:      cfg.Path,
		cpTesterFunc: checkpointTester,
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
	i := 0
	fn := m.getWALSize
	if m.walSizeFunc != nil {
		fn = m.walSizeFunc
	}
	go utils.CtxFireLoopTicker(sizeCtx, m.tickerFunc(), func() {
		// possible for ticker to invoke another loop before cancel makes it to the Done channel
		if i > 4 {
			return
		}
		size, err := fn(m.walPath)
		if err != nil {
			events.Log("error retrieving wal stat, %s", err)
			i++
			if i > 4 {
				// cancel to prevent log spamming
				events.Log("canceling WAL size monitoring due to consistent error, %s", err)
				sizeCancel()
			}
			return
		}
		stats.Set("wal-file-size", size)
		i = 0
	})

	x := 0
	cpCtx, cpCancel := context.WithCancel(ctx)
	utils.CtxFireLoopTicker(cpCtx, m.tickerFunc(), func() {
		// possible for ticker to invoke another loop before cancel makes it to the Done channel
		if x > 4 {
			return
		}
		res, err := m.cpTesterFunc()
		if err != nil {
			events.Log("error checking wal's checkpoint status, %s", err)
			x++
			if x > 4 {
				// cancel to prevent log spamming
				events.Log("canceling WAL checkpoint monitoring due to consistent error, %s", err)
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
		x = 0
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
