package event

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/ctlstore/pkg/tests"
	_ "github.com/segmentio/events/v2/text"
	"github.com/stretchr/testify/require"
)

// This test ensures that the reader can handle a partial JSON in the
// file that's caused by a writer that's still trying to write the
// data to the changelog.
func TestPartialReadChangelog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	f, teardown := tests.WithTmpFile(t, "changelog")
	defer teardown()

	cl := newFileChangelog(f.Name())
	err := cl.start(ctx)
	require.NoError(t, err)

	origEntry := entry{
		Seq:    42,
		Family: "my-family",
		Table:  "my-table",
		Key: []Key{
			{
				Name:  "my-column1",
				Type:  "string",
				Value: "test value",
			},
		},
	}

	serialized, err := json.Marshal(origEntry)
	require.NoError(t, err)
	pivot := len(serialized) / 2
	part1, part2 := serialized[:pivot], serialized[pivot:]

	// ensure that both of these parts are not valid json
	for _, part := range [][]byte{part1, part2} {
		var res entry
		err := json.Unmarshal(part, &res)
		require.Error(t, err)
	}

	// write the parts with some delay in between
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, err := f.Write(part1)
		require.NoError(t, err)
		wg.Done()
		time.Sleep(500 * time.Millisecond)
		part2 = append(part2, '\n') // the writer always appends a newline
		_, err = f.Write(part2)
		require.NoError(t, err)
	}()

	wg.Wait() // wait for the first write to finish
	event, err := cl.next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, origEntry.event(), event)

}

// This test ensures that the changelog reader can keep up with a log
// file that is periodically rotated either based on number of events
// written or file size.
//
// It uses a *fakeLogWriter instead of the *ldbWriterWithChangelog so
// that we have more control over the log being written wrt
// rotation semantics.
func TestChangelog(t *testing.T) {
	for _, test := range []struct {
		name             string
		numEvents        int
		writeDelay       time.Duration // writer delay between writing events
		rotateAfter      int           // how many events to write before rotating
		rotateAfterBytes int           // how many bytes to write before rotating
		timeout          time.Duration // if > 0, custom timeout per scenario
		mustRotateN      int           // if > 0, how often the file should have rotated
	}{
		{
			name:      "no rotation 1",
			numEvents: 10,
		},
		{
			name:      "no rotation 2",
			numEvents: 500,
		},
		{
			name:      "no rotation 3",
			numEvents: 5000,
		},
		{
			name:        "manual rotation",
			numEvents:   100,
			rotateAfter: 50,
			writeDelay:  1 * time.Millisecond,
		},
		{
			name:        "manual rotation 2",
			numEvents:   500,
			rotateAfter: 50,
			writeDelay:  1 * time.Millisecond,
		},
		{
			name:        "manual rotation 3",
			numEvents:   5000,
			rotateAfter: 500,
			writeDelay:  100 * time.Microsecond,
		},
		{
			name:             "size based 1",
			numEvents:        5000,
			rotateAfterBytes: 1024 * 128,
			writeDelay:       100 * time.Microsecond,
			mustRotateN:      4,
		},
		{
			name:             "size based 2",
			numEvents:        10000,
			rotateAfterBytes: 1024 * 128,
			writeDelay:       100 * time.Microsecond,
			mustRotateN:      8,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			timeout := test.timeout
			if timeout <= 0 {
				timeout = 10 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			f, teardown := tests.WithTmpFile(t, "changelog")
			defer teardown()

			cl := newFileChangelog(f.Name())
			err := cl.start(ctx)
			require.NoError(t, err)

			flw := &fakeLogWriter{
				path:             f.Name(),
				family:           "my-family",
				table:            "my-table",
				delay:            test.writeDelay,
				rotateAfter:      test.rotateAfter,
				rotateAfterBytes: test.rotateAfterBytes,
			}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := flw.writeN(ctx, test.numEvents)
				if err != nil {
					t.Fatal(err)
				}
			}()
			defer wg.Wait()
			for i := 0; i < test.numEvents; i++ {
				event, err := cl.next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				require.EqualValues(t, int64(i), event.Sequence)
			}

			if test.mustRotateN > 0 {
				require.EqualValues(t, test.mustRotateN, atomic.LoadInt64(&flw.rotations))
			}
		})
	}
}
func TestEncodeToWriter(t *testing.T) {
	entry := struct {
		Name string `json:"n"`
	}{
		Name: "foo",
	}

	var w bytes.Buffer
	err := json.NewEncoder(&w).Encode(entry)
	require.NoError(t, err)
	err = json.NewEncoder(&w).Encode(entry)
	require.NoError(t, err)

	// assert that it writes out a newline after every entry
	output := w.String()
	require.Equal(t,
		strings.Join([]string{`{"n":"foo"}`, `{"n":"foo"}`}, "\n")+"\n",
		output)
}

func TestEncodeToBufferedWriter(t *testing.T) {
	entry := struct {
		Name string `json:"n"`
	}{
		Name: "foo",
	}

	var w bytes.Buffer
	bw := bufio.NewWriter(&w)
	err := json.NewEncoder(bw).Encode(entry)
	require.NoError(t, err)
	err = json.NewEncoder(bw).Encode(entry)
	require.NoError(t, err)
	require.NoError(t, bw.Flush())

	// assert that it writes out a newline after every entry
	output := w.String()
	require.Equal(t,
		strings.Join([]string{`{"n":"foo"}`, `{"n":"foo"}`}, "\n")+"\n",
		output)
}

// Assserts behavior about the *bufio.Reader type
func TestReadBytesDelimiter(t *testing.T) {
	input := strings.Join([]string{"foo", "bar"}, "\n")
	reader := strings.NewReader(input)
	buf := bufio.NewReader(reader)
	b, err := buf.ReadBytes('\n')
	require.NoError(t, err)
	require.Equal(t, []byte("foo\n"), b) // verify that it includes the delimiter
	b, err = buf.ReadBytes('\n')
	require.Equal(t, io.EOF, err)
	require.Equal(t, []byte("bar"), b)
}
