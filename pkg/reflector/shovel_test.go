package reflector

import (
	"context"
	"database/sql"
	"errors"
	"github.com/segmentio/ctlstore/pkg/ldbwriter"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/events/v2"
	"reflect"
	"testing"
	"time"
)

// I totally overdid this test suite. I don't even know why.

type shovelTest struct {
	desc         string
	statements   []string
	source       dmlSource
	writer       ldbwriter.LDBWriter
	pollInterval time.Duration
	pollTimeout  time.Duration
	sourceDelay  time.Duration
	pre          func(*shovelTestContext)
	check        func(*shovelTestContext)
	expectErr    error
	timeout      time.Duration
	logArgs      events.Args
}

type shovelTestContext struct {
	st         *shovelTest
	srcDb      *sql.DB
	ldb        *sql.DB
	shovel     *shovel
	mockWriter *mockLdbWriter
	mockSource *mockDmlSource
	ctx        context.Context
	cancel     func()
}

type mockDmlSource struct {
	returnErr      error
	statements     []string
	injectInterval time.Duration
	lastInjection  time.Time
	delayFor       time.Duration
	buffer         []string
	seq            int64
	callCount      int
}

func (s *mockDmlSource) Next(ctx context.Context) (statement schema.DMLStatement, err error) {
	s.callCount++

	select {
	case <-ctx.Done():
		return schema.DMLStatement{}, ctx.Err()
	case <-time.After(s.delayFor):
	}

	if s.returnErr != nil {
		err = s.returnErr
		return
	}

	if time.Now().After(s.lastInjection.Add(s.injectInterval)) {
		for _, st := range s.statements {
			s.buffer = append(s.buffer, st)
		}

		// injectInterval zero value = don't re-inject
		if s.injectInterval != 0 {
			s.lastInjection = time.Now()
		} else {
			s.lastInjection = time.Now().Add(1000 * time.Hour)
		}
	}

	if len(s.buffer) < 1 {
		err = errNoNewStatements
		return
	}

	statementStr := s.buffer[0]
	s.buffer = s.buffer[1:]
	s.seq++
	statement = schema.DMLStatement{Statement: statementStr, Sequence: schema.DMLSequence(s.seq)}
	return
}

type mockLdbWriter struct {
	returnErr  error
	applied    []schema.DMLStatement
	appliedStr []string
}

func (w *mockLdbWriter) ApplyDMLStatement(ctx context.Context, statement schema.DMLStatement) error {
	if w.returnErr != nil {
		return w.returnErr
	}
	w.applied = append(w.applied, statement)
	w.appliedStr = append(w.appliedStr, statement.Statement)
	return nil
}

func TestShovel(t *testing.T) {
	errTest := errors.New("test")

	tests := []shovelTest{
		{
			desc: "Push 3 statements in, check if they applied",
			statements: []string{
				"HELLO WORLD 1",
				"HELLO WORLD 2",
				"HELLO WORLD 3",
			},
			check: func(tcx *shovelTestContext) {
				if !reflect.DeepEqual(tcx.st.statements, tcx.mockWriter.appliedStr) {
					t.Errorf("Expected to apply %v, but applied %v", tcx.st.statements, tcx.mockWriter.appliedStr)
				}
			},
			expectErr: context.DeadlineExceeded,
		},
		{
			desc:       "Errors percolate from ApplyDMLStatement",
			statements: []string{"HELLO WORLD"},
			pre: func(tcx *shovelTestContext) {
				tcx.mockWriter.returnErr = errTest
			},
			expectErr: errTest,
		},
		{
			desc: "Errors percolate from dmlSource.Next()",
			pre: func(tcx *shovelTestContext) {
				tcx.mockSource.returnErr = errTest
			},
			expectErr: errTest,
			logArgs:   events.Args{{"test", "value"}},
		},
		{
			desc:         "Polls at the specified interval",
			statements:   []string{"HELLO WORLD"},
			timeout:      55 * time.Millisecond,
			pollInterval: 50 * time.Millisecond,
			expectErr:    context.DeadlineExceeded,
			pre: func(tcx *shovelTestContext) {
				tcx.mockSource.injectInterval = 40 * time.Millisecond
			},
			check: func(tcx *shovelTestContext) {
				expect := []string{
					"HELLO WORLD",
					"HELLO WORLD",
				}
				if !reflect.DeepEqual(tcx.mockWriter.appliedStr, expect) {
					t.Errorf("Didn't get expected statements: %v, got: %v", expect, tcx.mockWriter.appliedStr)
				}
			},
		},
		{
			desc:         "Exits when context exits in NoNewStatements case",
			timeout:      50 * time.Millisecond,
			pollInterval: 100 * time.Millisecond,
			expectErr:    context.DeadlineExceeded,
		},
		{
			desc:       "Exits when context exits in main loop case",
			statements: []string{"Hello World"},
			timeout:    50 * time.Millisecond,
			pre: func(tcx *shovelTestContext) {
				tcx.cancel()
			},
			expectErr: context.Canceled,
		},
		{
			desc:         "Properly handles source timeout",
			timeout:      55 * time.Millisecond,
			pollInterval: 1 * time.Millisecond,
			statements:   []string{"HELLO WORLD"},
			// pollTimeout < timeout should allow the source.Next() call
			// to timeout, but the overall Start() call stays alive long
			// enough to capture it.
			pollTimeout: 10 * time.Millisecond,
			// sourceDelay > pollTimeout means source.Next() will always
			// hit DeadlineExceeded
			sourceDelay: 20 * time.Millisecond,
			check: func(tcx *shovelTestContext) {
				if len(tcx.mockWriter.appliedStr) > 0 {
					t.Errorf("Expected zero statements to be applied, got %v",
						len(tcx.mockWriter.appliedStr))
				}

				// Call count can be calculated by the maximum amount of
				// time to wait for each Next() call to finish divided by
				// the total amount of time to let the entire thing run.
				wantCallCount := int(tcx.st.timeout / tcx.st.pollTimeout)

				if want, got := wantCallCount, tcx.mockSource.callCount; want != got {
					t.Errorf("Expected source.Next() to be called %v times, got %v",
						want, got)
				}
			},
			expectErr: context.DeadlineExceeded,
			logArgs:   events.Args{{"test", "value"}},
		},
	}

	for _, t1 := range tests {
		t.Run(t1.desc, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			pollTimeout := t1.pollTimeout
			if pollTimeout == 0 {
				pollTimeout = 5 * time.Second
			}

			shov := shovel{
				source:       t1.source,
				writer:       t1.writer,
				pollInterval: t1.pollInterval,
				pollTimeout:  pollTimeout,
				logArgs:      t1.logArgs,
			}

			stctx := shovelTestContext{
				shovel: &shov,
				st:     &t1,
			}

			if shov.source == nil {
				stctx.mockSource = &mockDmlSource{statements: t1.statements}
				shov.source = stctx.mockSource
			}

			if shov.writer == nil {
				stctx.mockWriter = &mockLdbWriter{}
				shov.writer = stctx.mockWriter
			}

			stctx.mockSource.delayFor = t1.sourceDelay

			timeout := t1.timeout
			if timeout == 0 {
				timeout = 10 * time.Millisecond
			}

			subctx, subcancel := context.WithTimeout(ctx, timeout)
			defer subcancel()

			stctx.ctx = subctx
			stctx.cancel = subcancel

			if t1.pre != nil {
				t1.pre(&stctx)
			}

			returnedErr := shov.Start(stctx.ctx)
			if returnedErr != t1.expectErr {
				t.Fatalf("Unexpected error from shovel.Start(): %v", returnedErr)
			}

			if t1.check != nil {
				t1.check(&stctx)
			}
		})
	}
}
