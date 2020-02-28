package ctlstore

import (
	"fmt"
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
	globalLDBDirPath  = DefaultCtlstorePath
	globalCLPath      = filepath.Join(DefaultCtlstorePath, DefaultChangelogFilename)
	globalLDBReadOnly = true
	globalReader      *LDBReader
	globalReaderMu    sync.RWMutex
)

func init() {
	envPath := os.Getenv("CTLSTORE_PATH")
	if envPath != "" {
		globalLDBDirPath = envPath
		globalCLPath = filepath.Join(envPath, DefaultChangelogFilename)
	}
	sqlite.InitDriver()
}

// ReaderForPath opens an LDB at the provided path and returns an LDBReader
// instance pointed at that LDB.
func ReaderForPath(dirPath string) (*LDBReader, error) {
	path := filepath.Join(dirPath, ldb.DefaultLDBFilename)
	_, err := os.Stat(path)
	switch {
	case os.IsNotExist(err):
		return nil, fmt.Errorf("no LDB found at %s", path)
	case err != nil:
		return nil, err
	}

	return newLDBReaderFromPath(dirPath, ldbVersioning)
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
			reader, err := ReaderForPath(globalLDBDirPath)
			if err != nil {
				return nil, err
			}
			globalReader = reader
		}
	}

	return globalReader, nil
}
