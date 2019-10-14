// This package provides global statics for ctlstore
package globalstats

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/segmentio/events"
	"github.com/segmentio/stats"
)

const (
	statsPrefix    = "ctlstore.global"
	defaultAppName = "unknown"
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
	samplePct   float64
	mut         sync.Mutex
	flusherStop chan struct{}
)

func Observe(name string, value interface{}, tags ...stats.Tag) {
	if engine == nil {
		return
	}
	if rand.Float64() > samplePct {
		return
	}
	engine.Observe(name, value, tags...)
}

func Incr(name string, tags ...stats.Tag) {
	if engine == nil {
		return
	}
	engine.Incr(name, tags...)
}

// Disable turns off all globalstats behavior.  Calls to Observe and Incr will
// effectively be no-ops.
func Disable() {
	mut.Lock()
	defer mut.Unlock()

	if flusherStop != nil {
		close(flusherStop)
	}
	engine = nil
}

// Initialize globalstats behavior.
func Initialize(ctx context.Context, config Config) {
	mut.Lock()
	defer mut.Unlock()

	if flusherStop != nil {
		// stop any goroutines from a previous Initialize
		close(flusherStop)
	}
	flusherStop = make(chan struct{})
	if config.AppName == "" {
		config.AppName = defaultAppName
	}
	if config.SamplePct <= 0 || config.SamplePct > 1 {
		// by default only sample 10% of the observations
		config.SamplePct = 0.10
	}
	samplePct = config.SamplePct
	err := func() error {
		var err error
		engine, err = buildEngine(config)
		if err != nil {
			return err
		}
		flushEvery := 10 * time.Second
		if config.FlushEvery > 0 {
			flushEvery = config.FlushEvery
		}
		go flusher(ctx, flusherStop, engine, flushEvery)
		return nil
	}()
	if err != nil {
		events.Log("Could not initialize ctlstore global stats: %{error}s", err)
	}
}

func flusher(ctx context.Context, stop chan struct{}, flusher stats.Flusher, flushEvery time.Duration) {
	defer engine.Flush()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-time.After(flushEvery):
			flusher.Flush()
		}
	}
}

func buildEngine(config Config) (*stats.Engine, error) {
	handler := config.StatsHandler
	if handler == nil {
		return nil, errors.New("no stats handler supplied")
	}
	tags := []stats.Tag{
		{Name: "app", Value: config.AppName},
		{Name: "version", Value: config.CtlstoreVersion},
	}
	engine = stats.NewEngine(statsPrefix, handler, tags...)
	return engine, nil
}
