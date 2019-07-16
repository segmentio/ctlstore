package utils

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// EnsureDirForFile ensures that the specified file's parent directory
// exists.
func EnsureDirForFile(file string) error {
	dir := filepath.Dir(file)
	_, err := os.Stat(dir)
	switch {
	case err == nil:
		return nil
	case os.IsNotExist(err):
		err = os.Mkdir(dir, 0700)
		return errors.Wrapf(err, "mkdir %s", dir)
	default:
		return errors.Wrapf(err, "stat %s", dir)
	}
}
