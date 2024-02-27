package reflector

import (
	"context"
	"io"
	"time"

	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/ldbwriter"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/errors-go"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"
)

type shovel struct {
	source            dmlSource
	closers           []io.Closer
	writer            ldbwriter.LDBWriter
	pollInterval      time.Duration
	pollTimeout       time.Duration
	jitterCoefficient float64
	abortOnSeqSkip    bool
	maxSeqOnStartup   int64
	stop              chan struct{}
	log               *events.Logger
}

func (s *shovel) Start(ctx context.Context) error {
	jitr := newJitter()

	var cancel context.CancelFunc
	safeCancel := func() {
		if cancel != nil {
			cancel()
		}
	}

	var lastSeq schema.DMLSequence

	// Only actually close out the final cancel
	defer safeCancel()

	for {
		// early exit here if the shovel should be stopped
		select {
		case <-s.stop:
			s.logger().Log("Shovel stopping normally")
			return nil
		default:
		}

		// Need to clean up the cancel for each call of the loop, to avoid
		// leaking context.
		safeCancel()
		var sctx context.Context
		sctx, cancel = context.WithTimeout(ctx, s.pollTimeout)
		safeCancel = func() {
			if cancel != nil {
				cancel()
			}
		}

		stats.Incr("shovel.loop_enter")
		s.logger().Debug("shovel polling...")
		st, err := s.source.Next(sctx)

		if err != nil {
			causeErr := errors.Cause(err)
			if causeErr != context.DeadlineExceeded && causeErr != errNoNewStatements {
				return err
			}

			if causeErr == context.DeadlineExceeded {
				errs.Incr("shovel.deadline_exceeded")
			}

			//
			// The sctx deadline will trigger the DeadlineExceeded err, which
			// would happen in the case that the backing store for the source
			// is slow.
			//
			// Otherwise, errNoNewStatements is a positive assertion that the
			// no new statements have been found.
			//

			pollSleep := jitr.Jitter(s.pollInterval, s.jitterCoefficient)
			s.logger().Debug("Poll sleep %{sleepTime}s", pollSleep)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollSleep):
				// sctx timeouts will fall through here, so we should probably
				// TODO: add exponential backoff for retries
			}
			continue
		}

		s.logger().Debug("Shovel applying %{statement}v", st)

		if lastSeq != 0 {
			if st.Sequence > lastSeq+1 && st.Sequence.Int() > s.maxSeqOnStartup {
				stats.Incr("shovel.skipped_sequence")
				s.logger().Log("shovel skip sequence from:%{fromSeq}d to:%{toSeq}d", lastSeq, st.Sequence)

				if s.abortOnSeqSkip {
					// Mitigation for a bug that we haven't found yet
					stats.Incr("shovel.skipped_sequence_abort")
					err = errors.New("shovel skipped sequence")
					err = errors.WithTypes(err, "SkippedSequence")
					return err
				}
			}
		}

		// there's actually a statement to work
		err = s.writer.ApplyDMLStatement(ctx, st)
		if err != nil {
			errs.Incr("shovel.apply_statement.error")
			return errors.Wrapf(err, "ledger seq: %d", st.Sequence)
		}

		lastSeq = st.Sequence

		stats.Incr("shovel.apply_statement.success")

		// check if the context is done each loop
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// non-blocking
		}
	}
}

func (s *shovel) Close() error {
	for _, closer := range s.closers {
		err := closer.Close()
		if err != nil {
			s.logger().Log("shovel encountered error during close: %{error}s", err)
		}
	}
	return nil
}

func (s *shovel) logger() *events.Logger {
	if s.log == nil {
		s.log = events.DefaultLogger
	}
	return s.log
}
