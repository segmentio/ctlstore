package ctlstore

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/segmentio/ctlstore/pkg/ldb"
	_ "modernc.org/sqlite" // gives us sqlite3 everywhere
	// "github.com/segmentio/ctlstore/pkg/sqlite"
)

const (
	DefaultCtlstorePath        = "/var/spool/ctlstore/"
	DefaultChangelogFilename   = "change.log"
	defaultLDBVersioningSubdir = "versioned"
)

var (
	globalLDBDirPath           = DefaultCtlstorePath
	globalLDBVersioningDirPath = filepath.Join(DefaultCtlstorePath, defaultLDBVersioningSubdir)
	globalCLPath               = filepath.Join(DefaultCtlstorePath, DefaultChangelogFilename)
	globalLDBReadOnly          = true
	globalReader               *LDBReader
	globalReaderMu             sync.RWMutex
)

func init() {
	envPath := os.Getenv("CTLSTORE_PATH")
	if envPath != "" {
		globalLDBDirPath = envPath
		globalLDBVersioningDirPath = filepath.Join(envPath, defaultLDBVersioningSubdir)
		globalCLPath = filepath.Join(envPath, DefaultChangelogFilename)
	}
	// sqlite.InitDriver()
}

// ReaderForPath opens an LDB at the provided path and returns an LDBReader
// instance pointed at that LDB.
func ReaderForPath(path string) (*LDBReader, error) {
	return newLDBReader(path)
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
			var reader *LDBReader
			var err error
			if ldbVersioning {
				reader, err = newVersionedLDBReader(globalLDBVersioningDirPath)
			} else {
				reader, err = newLDBReader(filepath.Join(globalLDBDirPath, ldb.DefaultLDBFilename))
			}
			if err != nil {
				return nil, err
			}
			globalReader = reader
		}
	}

	return globalReader, nil
}
