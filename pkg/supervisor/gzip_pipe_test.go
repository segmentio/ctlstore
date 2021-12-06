package supervisor

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGZIPPipeReader(t *testing.T) {
	input := "hello world"
	var reader io.Reader = strings.NewReader(input)
	reader = newGZIPCompressionReader(reader)
	deflated, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	reader, err = gzip.NewReader(bytes.NewReader(deflated))
	require.NoError(t, err)
	inflated, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	require.EqualValues(t, input, string(inflated))
}

func TestGZIPPipeReaderErr(t *testing.T) {
	for _, test := range []struct {
		name     string
		input    io.ReadCloser
		readErr  error
		closeErr error
		expected error
	}{
		{
			name:  "no err",
			input: ioutil.NopCloser(strings.NewReader("hello, world")),
		},
		{
			name:     "read err",
			input:    ioutil.NopCloser(strings.NewReader("hello, world")),
			readErr:  errors.New("read failed"),
			expected: errors.New("copy to gzip writer: read failed"),
		},
		{
			name:     "close err",
			input:    ioutil.NopCloser(strings.NewReader("hello, world")),
			closeErr: errors.New("close failed"),
			expected: nil, // the gzip pipe reader should not close the input reader
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeReadCloser{
				rc:       test.input,
				readErr:  test.readErr,
				closeErr: test.closeErr,
			}
			reader := newGZIPCompressionReader(fake)
			_, err := ioutil.ReadAll(reader)
			if test.expected == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, test.expected.Error())
				require.True(t, fake.readCalled.IsSet())
				require.False(t, fake.closeCalled.IsSet())
			}
		})
	}
}

// TestIOPipes serves as a reference on how to use this damn thing.
func TestIOPipes(t *testing.T) {
	const bufSize = 100 * 1024
	data := make([]byte, bufSize)

	var reader io.Reader

	// verify that the entire payload is read uncompressed

	reader = bytes.NewReader(data)
	deflated, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, bufSize, len(deflated))

	// read the bytes as gzip

	pr, pw := io.Pipe()
	gw := gzip.NewWriter(pw)
	go func() {
		pw.CloseWithError(func() error {
			_, err := io.Copy(gw, bytes.NewReader(data))
			if err != nil {
				return fmt.Errorf("copy to gw: %w", err)
			}
			if err = gw.Close(); err != nil {
				return fmt.Errorf("close gzip writer: %w", err)
			}
			return nil
		}())
	}()

	deflated, err = ioutil.ReadAll(pr)
	require.NoError(t, err)
	require.True(t, len(deflated) < bufSize, "source=%d res=%d", bufSize, len(deflated))

	reader, err = gzip.NewReader(bytes.NewReader(deflated))
	require.NoError(t, err)
	inflated, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	require.EqualValues(t, data, inflated)
}
