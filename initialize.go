package ctlstore

import (
	"context"
	"os"

	"github.com/segmentio/ctlstore/pkg/globalstats"
	"github.com/segmentio/stats"
)

// by default, initialize ctlstore with some sane defaults
func init() {
	appName := os.Args[0]
	statsHandler := stats.DefaultEngine.Handler
	Initialize(context.Background(), appName, statsHandler)
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
