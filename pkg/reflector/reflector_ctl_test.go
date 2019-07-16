package reflector

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/ctlstore/pkg/reflector/fakes"
	"github.com/stretchr/testify/require"
)

func TestReflectorCtlAppContextCloses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reflector := fakes.NewFakeReflector()
	ctl := NewReflectorCtl(reflector)

	// kill the context once it's started
	cancel()

	// verify that starting the reflector with a canceled context
	// does not actually start the reflector
	ctl.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	require.EqualValues(t, 0, len(reflector.Events))
}

func TestReflectorCtl(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reflector := fakes.NewFakeReflector()
	ctl := NewReflectorCtl(reflector)

	// we should be able to stop the reflector before it starts.
	// these are no-ops.
	for i := 0; i < 5; i++ {
		ctl.Stop(ctx)
	}

	ctl.Start(ctx)

	// verify that the underlying reflector was started
	require.Equal(t, "started", reflector.NextEvent(ctx))

	// once started, starting again should be a no-op
	for i := 0; i < 5; i++ {
		ctl.Start(ctx)
	}
	require.EqualValues(t, 0, len(reflector.Events))

	ctl.Stop(ctx)
	require.Equal(t, "stopped", reflector.NextEvent(ctx))

	// once stopped, stopping again should be a no-op
	for i := 0; i < 5; i++ {
		ctl.Stop(ctx)
	}
	require.EqualValues(t, 0, len(reflector.Events))

	// restart and stop it again
	ctl.Start(ctx)
	require.Equal(t, "started", reflector.NextEvent(ctx))
	ctl.Stop(ctx)
	require.Equal(t, "stopped", reflector.NextEvent(ctx))

	// start it again, and then close it.
	ctl.Start(ctx)
	require.Equal(t, "started", reflector.NextEvent(ctx))
	require.NoError(t, ctl.Close())
	// closing the reflector involves first stopping it,
	// and then closing it.
	require.Equal(t, "stopped", reflector.NextEvent(ctx))
	require.Equal(t, "closed", reflector.NextEvent(ctx))

	// once closed, we should no longer be able to start
	// the reflector again.
	require.Panics(t, func() {
		ctl.Start(ctx)
	})
}

// ensure that when a parent context is canceled it also cancels
// the children contexts.  cancel funcs only cancel their own
// context root.
func TestReflectorCtlContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx2, _ := context.WithCancel(ctx)
	cancel()
	select {
	case <-ctx2.Done():
	default:
		t.Fatal("context should have been canceled")
	}
}
