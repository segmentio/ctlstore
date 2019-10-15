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

func Disable() {
	mut.Lock()
	defer mut.Unlock()

	if flusherStop != nil {
		close(flusherStop)
		flusherStop = nil
	}
}

func Initialize(ctx context.Context, config Config) {
	mut.Lock()
	defer mut.Unlock()

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

	samplePct = config.SamplePct

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

	go flusher(ctx, flusherStop, config.FlushEvery)
}

func flusher(ctx context.Context, stop <-chan struct{}, flushEvery time.Duration) {
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
