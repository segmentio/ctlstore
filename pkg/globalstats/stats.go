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
	engine    *stats.Engine
	once      sync.Once
	samplePct float64
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

func Initialize(ctx context.Context, config Config) {
	once.Do(func() {
		if config.AppName == "" {
			config.AppName = "unknown"
		}
		if config.SamplePct <= 0 || config.SamplePct > 1 {
			// by default only sample 10% of the observations
			config.SamplePct = 0.10
		}
		samplePct = config.SamplePct
		err := func() error {
			client := config.StatsHandler
			if client == nil {
				return errors.New("no datadog client supplied")
			}
			tags := []stats.Tag{
				{Name: "app", Value: config.AppName},
				{Name: "version", Value: config.CtlstoreVersion},
			}
			engine = stats.NewEngine(statsPrefix, client, tags...)
			flushEvery := 10 * time.Second
			if config.FlushEvery > 0 {
				flushEvery = config.FlushEvery
			}
			go func() {
				defer engine.Flush()
				for {
					select {
					case <-time.After(flushEvery):
						engine.Flush()
					case <-ctx.Done():
						return
					}
				}
			}()
			return nil
		}()
		if err != nil {
			events.Log("Could not initialize ctlstore global stats: %{error}s", err)
		}
	})
}
