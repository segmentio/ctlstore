package ctlstore

import (
	"context"

	"github.com/segmentio/ctlstore/pkg/globalstats"
	"github.com/segmentio/stats/v4"
)

type Config struct {
	// Stats specifies the config for reporting stats to the global
	// ctlstore stats namespace.
	//
	// By default, global stats are enabled with a set of sane defaults.
	Stats *globalstats.Config

	// LDBVersioning, if enabled, will instruct ctlstore to look for
	// LDBs inside of timestamp-delimited folders, and ctlstore will
	// hot-reload new LDBs as they appear.
	//
	// By default, this is disabled.
	LDBVersioning bool
}

var ldbVersioning bool

func init() {
	// Enable globalstats by default.
	globalstats.Initialize(context.Background(), globalstats.Config{})
}

// InitializeWithConfig sets up global state for thing including global
// metrics globalstats data and possibly more as time goes on.
func InitializeWithConfig(ctx context.Context, cfg Config) {
	if cfg.Stats != nil {
		// Initialize globalstats with the provided configuration:
		globalstats.Initialize(ctx, *cfg.Stats)
	}
	ldbVersioning = cfg.LDBVersioning
}

// Initialize sets up global state for thing including global
// metrics globalstats data and possibly more as time goes on.
//
// Deprecated: see InitializeWithConfig
func Initialize(ctx context.Context, appName string, statsHandler stats.Handler) {
	globalstats.Initialize(ctx, globalstats.Config{
		AppName:      appName,
		StatsHandler: statsHandler,
	})
}
