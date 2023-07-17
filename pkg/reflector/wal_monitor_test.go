package reflector

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/ctlstore/pkg/ldbwriter"
)

type fake struct {
	size           int64
	err            error
	wg             sync.WaitGroup
	statCallCount  atomic.Int64
	checkCallCount atomic.Int64
}

func (f *fake) Stat() func(m *WALMonitor) {
	return func(m *WALMonitor) {
		m.walSizeFunc = func(p string) (int64, error) {
			defer f.wg.Done()
			f.statCallCount.Add(1)
			v, err := m.getWALSize(p)
			f.size = v
			f.err = err
			return v, err
		}
	}
}

func (f *fake) Ticker() func(m *WALMonitor) {
	return func(m *WALMonitor) {
		m.tickerFunc = func() *time.Ticker {
			return time.NewTicker(time.Second)
		}
	}
}

func (f *fake) Checkpointer() func(m *WALMonitor) {
	return func(m *WALMonitor) {
		m.cpTesterFunc = func() (*ldbwriter.PragmaWALResult, error) {
			f.checkCallCount.Add(1)
			return nil, fmt.Errorf("fail")
		}
	}
}

func TestWALMonitorSize(t *testing.T) {
	tmpdir := t.TempDir()
	f, err := os.CreateTemp(tmpdir, "*.ldb-wal")
	if err != nil {
		t.Fatal(err)
	}

	n, err := f.WriteString("some random bytes!")
	if err != nil {
		t.Fatal(err)
	}

	if f.Sync() != nil {
		t.Fatal(err)
	}

	var fake fake
	fake.wg.Add(1)
	mon := NewMonitor(MonitorConfig{
		PollInterval: time.Millisecond,
		Path:         f.Name(),
	}, nil, fake.Stat(), fake.Ticker(), fake.Checkpointer())

	ctx, cancel := context.WithCancel(context.Background())
	go mon.Start(ctx)
	// wait for fake stat call
	fake.wg.Wait()
	cancel()

	if fake.statCallCount.Load() == 0 {
		t.Errorf("Stat should have been called at least once")
	}

	if fake.checkCallCount.Load() == 0 {
		t.Errorf("Checkpoint should have been called at least once")
	}
	if fake.err != nil {
		t.Errorf("unexpected error on stat: %v", fake.err)
	}

	if int64(n) != fake.size {
		t.Errorf("expected file size of %d, got %d", n, fake.size)
	}
}

func TestNoWALPath(t *testing.T) {
	var fake fake
	mon := NewMonitor(MonitorConfig{
		PollInterval: time.Millisecond,
		Path:         "",
	}, nil, fake.Stat(), fake.Ticker(), fake.Checkpointer())

	mon.Start(context.Background())

	if fake.statCallCount.Load() != 0 {
		t.Errorf("Stat should not have been called")
	}

	if fake.checkCallCount.Load() != 0 {
		t.Errorf("Checkpoint should not have been called")
	}
}

func TestWALMonitorStopsOnError(t *testing.T) {
	var fake fake
	fake.wg.Add(5)
	mon := NewMonitor(MonitorConfig{
		PollInterval: 50 * time.Microsecond,
		Path:         "path.ldb",
	}, nil, fake.Stat(), fake.Checkpointer())

	mon.Start(context.Background())
	fake.wg.Wait()
	if fake.statCallCount.Load() != 5 {
		t.Errorf("Stat should have been called 5 times, got %d", fake.statCallCount)
	}

	if fake.checkCallCount.Load() != 5 {
		t.Errorf("Checkpoint should have have been called 5 times, got %d", fake.checkCallCount)
	}
}
