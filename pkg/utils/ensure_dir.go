package utils

import (
	"fmt"
	"os"
	"path/filepath"
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
		return fmt.Errorf("mkdir %s: %w", dir, err)
	default:
		return fmt.Errorf("stat %s: %w", dir, err)
	}
}
