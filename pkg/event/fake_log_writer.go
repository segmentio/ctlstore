package event

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"time"

	"github.com/segmentio/errors-go"
	"github.com/segmentio/events/v2"
)

type fakeLogWriter struct {
	path             string
	family           string
	table            string
	delay            time.Duration
	rotateAfter      int
	rotateAfterBytes int
	seq              int64
	rotations        int64
}

// writes N events to the log
func (w *fakeLogWriter) writeN(ctx context.Context, n int) error {
	f, err := os.Create(w.path)
	if err != nil {
		return errors.Wrap(err, "create file")
	}
	defer func() {
		events.Debug("Done writing %{num}d events", n)
		if err == nil {
			err = f.Close()
		}
	}()
	bw := bufio.NewWriter(f)
	total := 0
	these := 0
	for total < n && ctx.Err() == nil {
		total++
		these++
		entry := entry{
			Seq:    atomic.LoadInt64(&w.seq),
			Family: w.family,
			Table:  w.table,
			Key: []Key{
				{
					Name:  "id-column",
					Type:  "int",
					Value: 42,
				},
			},
		}
		atomic.AddInt64(&w.seq, 1)
		err := json.NewEncoder(bw).Encode(entry)
		if err != nil {
			return errors.Wrap(err, "write event")
		}
		if err := bw.Flush(); err != nil {
			return errors.Wrap(err, "flush")
		}
		time.Sleep(w.delay)

		doRotate := false
		if w.rotateAfterBytes > 0 {
			info, err := os.Stat(w.path)
			if err != nil {
				return errors.Wrap(err, "stat path")
			}
			// fmt.Println(info.Size(), w.rotateAfterBytes)
			if info.Size() > int64(w.rotateAfterBytes) {
				events.Log("Rotation required (file size is %{bytes}d seq=%{seq}d)", info.Size(), atomic.LoadInt64(&w.seq))
				doRotate = true
			}
		}
		if w.rotateAfter > 0 && these >= w.rotateAfter {
			doRotate = true
		}

		if doRotate {
			events.Debug("Rotating log file..")
			these = 0
			if err := f.Close(); err != nil {
				return errors.Wrap(err, "close during rotation")
			}
			if err := os.Remove(f.Name()); err != nil {
				return errors.Wrap(err, "remove file")
			}
			f, err = os.Create(w.path)
			if err != nil {
				return errors.Wrap(err, "rotate into new file")
			}
			bw = bufio.NewWriter(f)
			atomic.AddInt64(&w.rotations, 1)
		}
	}
	return nil
}
