package logwriter

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func newSLWTestPath(t *testing.T) (path string, teardown func()) {
	f, err := ioutil.TempFile("", "sized-log-writer-test")
	require.NoError(t, err)
	return f.Name(), func() {
		os.Remove(f.Name())
	}
}

func TestSizedLogWriterCreatesFile(t *testing.T) {
	path, teardown := newSLWTestPath(t)
	defer teardown()
	w := SizedLogWriter{
		RotateSize: 100000,
		Path:       path,
	}
	defer w.Close()

	w.WriteLine("hello")

	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}

	if diff := cmp.Diff([]byte("hello\n"), bytes); diff != "" {
		t.Errorf("Bytes differ\n%v", diff)
	}
}

func TestSizedLogWriterAppendsToExistingFile(t *testing.T) {
	path, teardown := newSLWTestPath(t)
	defer teardown()
	err := ioutil.WriteFile(path, []byte("line1\n"), 0644)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}

	w := SizedLogWriter{
		RotateSize: 100000,
		Path:       path,
	}
	defer w.Close()

	w.WriteLine("line2")

	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}

	if diff := cmp.Diff([]byte("line1\nline2\n"), bytes); diff != "" {
		t.Errorf("Bytes differ\n%v", diff)
	}
}

func TestSizedLogWriterRotatesFile(t *testing.T) {
	path, teardown := newSLWTestPath(t)
	defer teardown()
	err := ioutil.WriteFile(path, []byte("1234567890\n"), 0644)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}

	w := SizedLogWriter{
		RotateSize: 21, // chosen so it will rotate right at the third
		Path:       path,
	}
	defer w.Close()

	w.WriteLine("1234567890")
	w.WriteLine("1234567890")

	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}

	if diff := cmp.Diff([]byte("1234567890\n"), bytes); diff != "" {
		t.Errorf("Bytes differ\n%v", diff)
	}
}
