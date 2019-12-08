package ctlstore

import (
	"context"

	"github.com/segmentio/ctlstore/pkg/globalstats"
	"github.com/segmentio/stats/v4"
)

// Initialize setup up global state for thing including global
// metrics globalstats data and possibly more as time goes on.
func Initialize(ctx context.Context, appName string, statsHandler stats.Handler) {
	globalstats.Initialize(ctx, globalstats.Config{
		AppName:         appName,
		StatsHandler:    statsHandler,
		CtlstoreVersion: Version,
	})
}
