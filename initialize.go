package ctlstore

import (
	"context"

	"github.com/segmentio/ctlstore/pkg/globalstats"
	"github.com/segmentio/stats"
)

// By default, initialize ctlstore with some sane defaults.
func init() {
	// Use the default global stats application name.
	appName := ""
	Initialize(context.Background(), appName, stats.DefaultEngine.Handler)
}

// Initialize setup up global state for thing including global
// metrics globalstats data and possibly more as time goes on.
func Initialize(ctx context.Context, appName string, statsHandler stats.Handler) {
	globalstats.Initialize(ctx, globalstats.Config{
		AppName:         appName,
		StatsHandler:    statsHandler,
		CtlstoreVersion: Version,
	})
}
