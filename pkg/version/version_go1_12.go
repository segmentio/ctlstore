// +build go1.12

package version

import (
	"runtime/debug"
)

const path = "github.com/segmentio/ctlstore"

// The version is extracted from build information embedded in the binary, from
// a go.mod file, so this version field is only available in Go modules projects.
// We determine the version dynamically instead of using -ldflags to inject the
// version because ctlstore will be imported as a library, and we do not expect
// consumers to set ctlstore's version for us.
// Note: `debug.ReadBuildInfo` is only available in Go 1.12+, so we gate this
// with the build tag above.
func init() {
	if info, ok := debug.ReadBuildInfo(); ok && info != nil {
		if info.Main.Path == path {
			version = info.Main.Version
		} else {
			for _, mod := range info.Deps {
				if mod != nil {
					if mod.Path == path {
						version = mod.Version
					}
				}
			}
		}
	}
}
