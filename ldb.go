package ctlstore

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/sqlite"
)

const (
	DefaultCtlstorePath      = "/var/spool/ctlstore/"
	DefaultChangelogFilename = "change.log"
)

var (
	globalLDBPath     = filepath.Join(DefaultCtlstorePath, ldb.DefaultLDBFilename)
	globalCLPath      = filepath.Join(DefaultCtlstorePath, DefaultChangelogFilename)
	globalLDBReadOnly = true
	globalReader      *LDBReader
	globalReaderMu    sync.RWMutex
)

func init() {
	envPath := os.Getenv("CTLSTORE_PATH")
	if envPath != "" {
		globalLDBPath = filepath.Join(envPath, ldb.DefaultLDBFilename)
		globalCLPath = filepath.Join(envPath, DefaultChangelogFilename)
	}
	sqlite.InitDriver()
}

// ReaderForPath opens an LDB at the provided path and returns an LDBReader
// instance pointed at that LDB.
func ReaderForPath(path string) (*LDBReader, error) {
	mode := "ro"
	if !globalLDBReadOnly {
		mode = "rwc"
	}

	ldb, err := ldb.OpenLDB(path, mode)
	if err != nil {
		return nil, err
	}
	return &LDBReader{Db: ldb}, nil
}

// Reader returns an LDBReader that can be used globally.
func Reader() (*LDBReader, error) {
	globalReaderMu.RLock()
	defer globalReaderMu.RUnlock()

	if globalReader == nil {
		globalReaderMu.RUnlock()
		defer globalReaderMu.RLock()
		globalReaderMu.Lock()
		defer globalReaderMu.Unlock()

		if globalReader == nil {
			reader, err := ReaderForPath(globalLDBPath)
			if err != nil {
				return nil, err
			}
			globalReader = reader
		}
	}

	return globalReader, nil
}
