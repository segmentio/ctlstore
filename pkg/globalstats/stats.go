// Package globalstats provides configurable singleton stats instance for ctlstore.
package globalstats

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/segmentio/events"
	"github.com/segmentio/stats"
)

const (
	statsPrefix = "ctlstore.global"
)

// version will be set by CI using ld_flags to the git SHA on which the binary was built
var (
	version = "unknown"
)

type (
	Config struct {
		CtlstoreVersion string
		AppName         string // set this to your app name
		StatsHandler    stats.Handler
		FlushEvery      time.Duration
		SamplePct       float64
	}
)

var (
	engine      *stats.Engine
	mut         sync.Mutex
	flusherStop chan struct{}
	config      Config
	globalctx   context.Context
	stopped     bool
)

func Observe(name string, value interface{}, tags ...stats.Tag) {
	engine, ok := lazyInitializeEngine()
	if !ok {
		return
	}

	if rand.Float64() > config.SamplePct {
		return
	}
	engine.Observe(name, value, tags...)
}

func Incr(name string, tags ...stats.Tag) {
	engine, ok := lazyInitializeEngine()
	if !ok {
		return
	}

	engine.Incr(name, tags...)
}

func Disable() {
	mut.Lock()
	defer mut.Unlock()

	stopped = true

	// Stop any goroutines launched from any previous Initialize calls.
	if flusherStop != nil {
		close(flusherStop)
		flusherStop = nil
	}
}

func Initialize(ctx context.Context, cfg Config) {
	mut.Lock()
	defer mut.Unlock()

	config = cfg
	globalctx = ctx
}

func lazyInitializeEngine() (*stats.Engine, bool) {
	if stopped || engine != nil {
		return engine, !stopped
	}

	mut.Lock()
	defer mut.Unlock()

	// Perform a second check after grabbing the mutex, in case there was competition for the lock.
	if stopped || engine != nil {
		return engine, !stopped
	}

	if config.AppName == "" {
		if len(os.Args) > 0 {
			config.AppName = filepath.Base(os.Args[0])
		} else {
			config.AppName = "unknown"
		}
	}
	if config.SamplePct == 0 {
		// By default, only sample 10% of the observations.
		config.SamplePct = 0.10
	}
	if config.FlushEvery == 0 {
		config.FlushEvery = 10 * time.Second
	}
	if config.StatsHandler == nil {
		config.StatsHandler = stats.DefaultEngine.Handler
	}
	if config.CtlstoreVersion == "" {
		config.CtlstoreVersion = version
	}
	if globalctx == nil {
		globalctx = context.Background()
	}

	err := func() error {
		if config.SamplePct > 1 || config.SamplePct < 0 {
			return errors.New("sample percentage must be in the range of (0, 1]")
		}
		if config.FlushEvery < 0 {
			return errors.New("flush rate must be a positive duration")
		}

		return nil
	}()
	if err != nil {
		events.Log("Could not initialize ctlstore globalstats: %{error}s", err)
		return nil, false
	}

	// Stop any goroutines launched from any previous Initialize calls.
	if flusherStop != nil {
		close(flusherStop)
	}
	flusherStop = make(chan struct{})

	tags := []stats.Tag{
		{Name: "app", Value: config.AppName},
		{Name: "version", Value: config.CtlstoreVersion},
	}
	engine = stats.NewEngine(statsPrefix, config.StatsHandler, tags...)

	go flusher(globalctx, flusherStop, config.FlushEvery, engine)

	return engine, true
}

func flusher(ctx context.Context, stop <-chan struct{}, flushEvery time.Duration, engine *stats.Engine) {
	defer engine.Flush()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-time.After(flushEvery):
			engine.Flush()
		}
	}
}
