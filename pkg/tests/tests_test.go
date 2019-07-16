package tests

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithTmpDir(t *testing.T) {
	td1, td1td := WithTmpDir(t)
	td2, td2td := WithTmpDir(t)

	// they can't be the same dirs
	require.NotEqual(t, td1, td2)

	exists := func(path string) bool {
		_, err := os.Stat(path)
		return err == nil
	}
	require.True(t, exists(td1))
	require.True(t, exists(td2))

	td1td()
	require.False(t, exists(td1))
	require.True(t, exists(td2))

	td2td()
	require.False(t, exists(td1))
	require.False(t, exists(td2))
}

func TestWithTmpFile(t *testing.T) {
	f1, f1td := WithTmpFile(t, "foo")
	f2, f2td := WithTmpFile(t, "foo")

	require.NotEqual(t, f1.Name(), f2.Name())

	exists := func(file *os.File) bool {
		_, err := os.Stat(file.Name())
		return err == nil
	}
	require.True(t, exists(f1))
	require.True(t, exists(f2))

	f1td()
	require.False(t, exists(f1))
	require.True(t, exists(f2))

	f2td()
	require.False(t, exists(f1))
	require.False(t, exists(f2))

	f1.Close()
}
