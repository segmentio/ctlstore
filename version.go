package ctlstore

import (
	"github.com/segmentio/ctlstore/pkg/version"
)

// Version is the current ctlstore client library version.
var Version string

func init() {
	Version = version.Get()
}
