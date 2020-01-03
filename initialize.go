package ctlstore

import (
	"context"

	"github.com/segmentio/ctlstore/pkg/globalstats"
	"github.com/segmentio/stats/v4"
)

type Config struct {
	Stats globalstats.Config
}

func init() {
	// Enable globalstats by default.
	globalstats.Initialize(context.Background(), globalstats.Config{})
}

// InitializeWithConfig sets up global state for thing including global
// metrics globalstats data and possibly more as time goes on.
func InitializeWithConfig(ctx context.Context, cfg Config) {
	// Initialize globalstats with the provided configuration:
	globalstats.Initialize(ctx, cfg.Stats)
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
