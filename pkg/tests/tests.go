package tests

import (
	"context"
	"database/sql"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/ctlstore/pkg/ctldb"
	"github.com/segmentio/ctlstore/pkg/utils"
)

func WithTmpDir(t testing.TB) (dir string, teardown func()) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	return tmpDir, func() {
		os.RemoveAll(tmpDir)
	}
}

func WithTmpFile(t testing.TB, name string) (file *os.File, teardown func()) {
	var teardowns utils.Teardowns
	dir, teardown := WithTmpDir(t)
	teardowns.Add(teardown)

	path := filepath.Join(dir, name)
	var err error
	file, err = os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	teardowns.Add(func() { file.Close() })
	return file, teardowns.Teardown
}

func CheckCtldb(t *testing.T) {
	db, err := sql.Open("mysql", ctldb.GetTestCtlDBDSN(t))
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_, err = db.ExecContext(ctx, "SELECT 1")
		db.Close()
		if err == nil {
			return
		}
	}
	t.Fatalf(`
		*** Tests require MySQL to be up ***
		Error: %v"
		\n\nHINT: Have you ran 'docker-compose up'?\n\n
	`, err)
}
