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
	mut.Lock()
	defer mut.Unlock()

	lazyInitializeEngine()

	if stopped || engine == nil {
		return
	}
	if rand.Float64() > config.SamplePct {
		return
	}
	engine.Observe(name, value, tags...)
}

func Incr(name string, tags ...stats.Tag) {
	mut.Lock()
	defer mut.Unlock()

	lazyInitializeEngine()

	if stopped || engine == nil {
		return
	}
	engine.Incr(name, tags...)
}

func Disable() {
	mut.Lock()
	defer mut.Unlock()

	stopped = true

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

func lazyInitializeEngine() {
	if stopped || engine != nil {
		return
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

	err := func() error {
		if config.StatsHandler == nil {
			return errors.New("no datadog client supplied")
		}
		if config.SamplePct > 1 || config.SamplePct < 0 {
			return errors.New("sample percentage must be in the range of (0, 1]")
		}
		if config.FlushEvery < 0 {
			return errors.New("flush rate must be a positive duration")
		}
		if config.CtlstoreVersion == "" {
			return errors.New("must supply the ctlstore version")
		}

		return nil
	}()
	if err != nil {
		events.Log("Could not initialize ctlstore global stats: %{error}s", err)
		return
	}

	// Stop any goroutines from any previous Initialize calls.
	if flusherStop != nil {
		close(flusherStop)
	}
	flusherStop = make(chan struct{})

	tags := []stats.Tag{
		{Name: "app", Value: config.AppName},
		{Name: "version", Value: config.CtlstoreVersion},
	}
	engine = stats.NewEngine(statsPrefix, config.StatsHandler, tags...)

	defer flusher(globalctx, flusherStop, config.FlushEvery, engine)
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
