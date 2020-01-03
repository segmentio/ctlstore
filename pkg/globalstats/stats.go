// Package globalstats provides configurable singleton stats instance for ctlstore.
// It is heavily inspired by segmentio/flagon.
package globalstats

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/segmentio/ctlstore/pkg/version"
	"github.com/segmentio/errors-go"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"
)

const (
	statsPrefix      = "ctlstore.global"
	maxInflightStats = 1024
)

type (
	Config struct {
		AppName      string // set this to your app name
		StatsHandler stats.Handler
		FlushEvery   time.Duration
		// SamplePct is the percent of Observe calls to report.
		SamplePct float64
		ctx       context.Context
	}
	observation struct {
		name  string
		value interface{}
		tags  []stats.Tag
	}
	counterKey struct {
		name   string
		family string
		table  string
	}
	statEventType int
	// statEvent is a union type; only one of its values will be set, depending on the StatEventType.
	statEvent struct {
		typ     statEventType
		cfg     Config
		incr    counterKey
		observe observation
	}
)

const (
	_ statEventType = iota
	statEventTypeConfig
	statEventTypeIncr
	statEventTypeObserve
	statEventTypeClose
)

var (
	eventChan = make(chan statEvent, maxInflightStats)

	// This is a best-effort attempt to detect when we are dropping stats. This counter will
	// be incremented atomically when the stats channel is full. If the engine is configured,
	// then this value will be emitted as a separate metric.
	droppedStats int64
)

func init() {
	go loop()
}

func Incr(name, family, table string) {
	k := counterKey{name: name, family: family, table: table}
	select {
	case eventChan <- statEvent{typ: statEventTypeIncr, incr: k}:
	default:
		// eventChan is full, drop this stat
		incrDroppedStats()
	}
}

func Observe(name string, value interface{}, tags ...stats.Tag) {
	k := observation{name: name, value: value, tags: tags}
	select {
	case eventChan <- statEvent{typ: statEventTypeObserve, observe: k}:
	default:
		// eventChan is full, drop this stat
		incrDroppedStats()
	}
}

// Initialize configures ctlstore to report global stats. This must be called
// at least once in order to report stats.
func Initialize(ctx context.Context, cfg Config) {
	// Set any default configuration values, where necessary.
	if cfg.AppName == "" {
		if len(os.Args) > 0 {
			cfg.AppName = filepath.Base(os.Args[0])
		} else {
			cfg.AppName = "unknown"
		}
	}
	if cfg.SamplePct == 0 {
		// By default, only sample 10% of the observations.
		cfg.SamplePct = 0.10
	}
	if cfg.FlushEvery == 0 {
		cfg.FlushEvery = 10 * time.Second
	}
	if cfg.StatsHandler == nil {
		cfg.StatsHandler = stats.DefaultEngine.Handler
	}
	cfg.ctx = ctx
	if cfg.ctx == nil {
		cfg.ctx = context.Background()
	}

	// Validate that all config values are valid.
	if cfg.FlushEvery < 0 {
		events.Log("Could not initialize ctlstore global stats: %{error}s", errors.New("flush rate must be a positive duration"))
		return
	}

	eventChan <- statEvent{typ: statEventTypeConfig, cfg: cfg}
}

func Close() {
	eventChan <- statEvent{typ: statEventTypeClose}
}

func loop() {
	var cfg *Config
	var engine *stats.Engine
	var closed bool

	// Start with an empty ticker, which will start with a nil ticker channel.
	ticker := &time.Ticker{}
	m := make(map[counterKey]int64)
	ctx := context.Background()

	for {
		select {
		// If the program is shutting down, exit this goroutine.
		case <-ctx.Done():
			return

		// Flush stats on a regular basis:
		case <-ticker.C:
			if engine = lazyInitEngine(cfg, engine); engine == nil || closed {
				continue
			}

			// Emit our best-effort count of dropped stats since the last flush.
			engine.Add("dropped-stats", getDroppedStatsCount())

			// Emit aggregated Incr metrics.
			for k, v := range m {
				engine.Add(k.name, v, stats.T("family", k.family), stats.T("table", k.table))
				delete(m, k)
			}

			engine.Flush()

		// All stats events (Incr/Observe/Initialize/Close) are represented as a statEvent.
		// This allows us to remove the complexity around handling concurrent stats requests
		// by imposing an ordering to these events (based on when they are sent on eventChan).
		case event := <-eventChan:
			switch event.typ {

			// A new config was provided, so reconfigure the local state.
			case statEventTypeConfig:
				cfg = &event.cfg

				// Reset the flush ticker:
				if ticker.C != nil {
					ticker.Stop()
				}
				ticker = time.NewTicker(cfg.FlushEvery)

				// Store the newest context:
				ctx = cfg.ctx

				// Clear the engine, so that we build a new one on the next flush:
				engine = nil

				// Reset the closed variable so that we can start emitting stats again.
				// Probably unnecessary to handle this case, but seems harmless to support.
				closed = false

			// .Incr() was called; record the new stat.
			case statEventTypeIncr:
				if closed {
					continue
				}

				m[event.incr]++

			// .Observe() was called; record the new observation.
			case statEventTypeObserve:
				if engine = lazyInitEngine(cfg, engine); engine == nil || closed {
					continue
				}

				engine.Observe(event.observe.name, event.observe.value, event.observe.tags...)

			// We're shutting down stats, so stop recording and flushing metrics.
			case statEventTypeClose:
				closed = true
			}
		}
	}
}

func lazyInitEngine(cfg *Config, engine *stats.Engine) *stats.Engine {
	if engine != nil || cfg == nil {
		return engine
	}

	// We have to lazily initialize this engine because the default stats handler
	// (stats.DefaultEngine.Handler) defaults to stats.DiscardHandler until
	// the user overrides this during service initialization.
	handler := cfg.StatsHandler
	if handler == nil || handler == stats.Discard {
		handler = stats.DefaultEngine.Handler
	}
	if handler == stats.Discard {
		return nil
	}

	tags := []stats.Tag{
		{Name: "app", Value: cfg.AppName},
		{Name: "version", Value: version.Get()},
	}

	return stats.NewEngine(statsPrefix, handler, tags...)
}

// incrDroppedStats atomically records that a stat was dropped.
func incrDroppedStats() {
	atomic.AddInt64(&droppedStats, 1)
}

// getDroppedStatsCount returns the number of dropped stats since the last
// call to getDroppedStatsCount.
func getDroppedStatsCount() int64 {
	return atomic.SwapInt64(&droppedStats, 0)
}
