package event

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/errors-go"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"
)

type (
	// fileChangelog is the main implementation of changelog.  it continually reads
	// from the file changelog and detects when it has been rotated. it buffers
	// events in its composed events channel so that it can keep reading while events
	// are being processed.
	fileChangelog struct {
		path   string
		events chan eventErr
	}
	// eventErr simply composes an event or an error. the events chan is a channel of
	// this type, and is used to send errors encountered during reading the changelog
	// to the iterator, and ultimately, its client.
	eventErr struct {
		event Event
		err   error
	}
)

func newFileChangelog(path string) *fileChangelog {
	return &fileChangelog{
		path:   path,                      // the path of the changelog on disk
		events: make(chan eventErr, 1024), // probably good to have a buffer here
	}
}

// next produces the next changelog Event, using a context for cancellation.
// the iterator will call this method to produce events to its client.
func (c *fileChangelog) next(ctx context.Context) (Event, error) {
	select {
	case ee := <-c.events:
		return ee.event, ee.err
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
}

// start creates the fs watchers and starts reading from
// the on-disk changelog. this method does not block while
// the changelog is reading from the filesystem.
//
// The context should be canceled when the changelog is no
// longer needed.  If this method returns an error the
// *fileChangelog is not usable.
func (c *fileChangelog) start(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrap(err, "create fsnotify watcher")
	}
	go func() {
		select {
		case <-ctx.Done():
			if err := watcher.Close(); err != nil {
				events.Log("Could not close watcher: %{err}s", err)
			}
		}
	}()
	paths := []string{c.path, filepath.Dir(c.path)}
	for _, w := range paths {
		if err := watcher.Add(w); err != nil {
			return errors.Wrapf(err, "could not watch '%s'", w)
		}
	}
	fsNotifyCh := make(chan fsnotify.Event)
	fsErrCh := make(chan error)
	// fsnotify recommends reading the error and events chans from
	// separate goroutines. indeed, not doing this causes our tests
	// to fail.
	go func() {
		for {
			select {
			case err := <-watcher.Errors:
				events.Log("FS err: %{err}s", err)
				select {
				case fsErrCh <- err:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				// filter out events that are not related to our changelog since
				// we are also watching the parent dir of the changelog in order
				// to correctly detect file rotation.
				if event.Name == c.path {
					select {
					case fsNotifyCh <- event:
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	go c.read(ctx, fsNotifyCh, fsErrCh)
	return nil
}

// read continually reads from the changelog until the context is cancelled.
func (c *fileChangelog) read(ctx context.Context, fsNotifyCh chan fsnotify.Event, fsErrCh chan error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logCount := 0
	prevSeq := int64(-1)
	// each iteration opens the file and reads until it is rotated.
	for ctx.Err() == nil {
		// Read as much as possible until you hit EOF.  Then wait for a notification
		// from fsnotify or just check again later.
		err := func() error {
			// first, open the changelog
			f, err := os.Open(c.path)
			if err != nil {
				return errors.Wrap(err, "open changelog")
			}
			defer func() {
				events.Debug("Closing changelog...")
				if err := f.Close(); err != nil {
					errs.Incr("changelog-errors", stats.T("op", "close file"))
					events.Log("Could not close changelog file: %{error}s", err)
				}
			}()
			events.Debug("Opening changelog...")
			logCount = 3 // log first read seq # from new file

			br := bufio.NewReaderSize(f, 60*1024)

			handleEventData := func(b []byte, logSeq bool) {
				if len(b) == 0 {
					// don't bother
					return
				}
				var entry entry
				if err := json.Unmarshal(b, &entry); err != nil {
					c.send(ctx, eventErr{err: errors.Wrapf(err, "parse entry '%s'", b)})
					errs.Incr("changelog-errors", stats.T("op", "parse json"))
					return
				}
				event := entry.event()
				if logSeq {
					events.Log("prev event seq: %d", prevSeq)
					events.Log("cur event seq: %d", event.Sequence)
				}
				prevSeq = event.Sequence
				c.send(ctx, eventErr{event: event})
			}

			var buffer bytes.Buffer
			readEvents := func(c int) error {
				for {
					b, err := br.ReadBytes('\n')

					// if we hit an error, reset our buffer and quit
					if err != nil && err != io.EOF {
						buffer.Reset()
						return err
					}

					// add to the buffer
					buffer.Write(b)

					// check to see if the buffer ends with a newline
					if buffer.Len() > 0 {
						if buffer.Bytes()[buffer.Len()-1] == '\n' {
							shouldLog := c < 0 || c > 0
							if c > 0 {
								c--
							}
							handleEventData(buffer.Bytes(), shouldLog)
							buffer.Reset()
						}
					}

					if err != nil {
						return err
					}
				}
			}

			// loop and read as many lines as possible.
			for {
				err = readEvents(logCount)
				logCount = 0
				if err != io.EOF {
					return errors.Wrap(err, "read bytes")
				}
				select {
				case <-time.After(time.Second):
					//events.Debug("Manually checking log")
					continue
				case err := <-fsErrCh:
					events.Debug("err channel: %{error}s", err)
					if err := readEvents(-1); err != io.EOF {
						events.Log("could not consume rest of file: %{error}s", err)
					}
					return errors.Wrap(err, "watcher error")
				case event := <-fsNotifyCh:
					switch event.Op {
					case fsnotify.Write:
						continue
					case fsnotify.Create:
						events.Debug("New changelog created. Consuming the rest of current one...")
						err := readEvents(-1)
						if err != io.EOF {
							return errors.Wrap(err, "consume rest of changelog")
						}
						events.Debug("Restarting reader, last seq from old file: %d", prevSeq)
						return nil
					}
				case <-ctx.Done():
					events.Debug("Changelog context finished. Exiting.")
					return ctx.Err()
				}
			}
		}()
		switch {
		case err == nil:
			// loop again
		case errs.IsCanceled(err):
			return
		case os.IsNotExist(errors.Cause(err)):
			events.Log("Changelog file does not exist, rechecking...")
			select {
			case <-fsNotifyCh:
				events.Log("Changelog notified")
			case <-time.After(time.Second):
				events.Log("Manually checking")
			}
		default:
			errs.Incr("changelog-errors", stats.T("op", "open file"))
			c.send(ctx, eventErr{err: err})
			time.Sleep(time.Second)
		}
	} // for
}

// send attempts to send an eventErr on the c.event channel.
func (c *fileChangelog) send(ctx context.Context, ee eventErr) {
	select {
	case c.events <- ee:
	case <-ctx.Done():
	}
}

// validate ensures that the changelog exists before starting.
// if it does not already exist it will wait one second and
// try again.
func (c *fileChangelog) validate() error {
	_, err := os.Stat(c.path)
	switch {
	case err == nil:
	case os.IsNotExist(err):
		events.Log("changelog does not exist. waiting 1s for rotation")
		time.Sleep(time.Second)
		_, err = os.Stat(c.path)
		switch {
		case err == nil:
		case os.IsNotExist(err):
			return errors.Wrap(err, "changelog does not exist")
		default:
			return errors.Wrap(err, "stat changelog")
		}
	default:
		return errors.Wrap(err, "stat changelog")
	}
	return nil
}
