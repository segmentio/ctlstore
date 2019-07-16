package reflector

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/utils"
	"github.com/segmentio/events"
	"github.com/segmentio/stats"
)

const (
	// reflectorCtlTimeout is how long the ctl will wait on the reflector to
	// start, stop, etc.
	reflectorCtlTimeout = 5 * time.Second
)

type (
	// reflectorCtl controls starting and stopping the reflector
	// for the purposes of a supervisor being able to perform a
	// snapshot of the ldb without the reflector running
	// concurrently. When the snapshot must be made, it will stop
	// the reflector, perform the snapshot, and then start the
	// reflector again.
	ReflectorCtl struct {
		reflector ReflectorI
		closed    utils.AtomicBool
		messages  chan reflectorCtlMsg
		once      sync.Once
	}

	// used to send/receive messages to/from the reflectorCtl
	reflectorCtlMsg struct {
		desired bool       // true == start reflector, stop == stop reflector
		result  chan error // errors are returned on this chan
	}
	ReflectorI interface {
		Start(ctx context.Context) error
		Close() error
	}
)

func NewReflectorCtl(reflector ReflectorI) *ReflectorCtl {
	ctl := &ReflectorCtl{
		reflector: reflector,
		messages:  make(chan reflectorCtlMsg),
	}
	return ctl
}

func (r *ReflectorCtl) initLifecycle(ctx context.Context) {
	r.once.Do(func() {
		go r.lifecycle(ctx)
	})
}

// Start sends a start msg to the lifecycle goroutine. The lifecycle
// goroutine manages the state machine of the reflector. Since the
// lifecycle is a single goroutine it is last-write-wins.
func (r *ReflectorCtl) Start(ctx context.Context) {
	start := time.Now()
	defer func() {
		stats.Observe("reflector-ctl-latency", time.Now().Sub(start), stats.Tag{
			Name:  "op",
			Value: "start",
		})
	}()
	events.Log("Starting reflector")
	r.assertNotClosed()
	r.initLifecycle(ctx)
	result := make(chan error)
	select {
	case r.messages <- reflectorCtlMsg{desired: true, result: result}:
	case <-ctx.Done():
		return
	}
	select {
	case err := <-result:
		if err != nil {
			panic("could not start reflector: " + err.Error())
		}
	case <-ctx.Done():
		return
	}
}

// Stop sends a start msg to the lifecycle goroutine. The lifecycle
// goroutine manages the state machine of the reflector. Since the
// lifecycle is a single goroutine it is last-write-wins.
func (r *ReflectorCtl) Stop(ctx context.Context) {
	start := time.Now()
	defer func() {
		stats.Observe("reflector-ctl-latency", time.Now().Sub(start), stats.Tag{
			Name:  "op",
			Value: "stop",
		})
	}()
	events.Log("Stopping reflector")
	r.assertNotClosed()
	r.initLifecycle(ctx)
	result := make(chan error)
	select {
	case r.messages <- reflectorCtlMsg{desired: false, result: result}:
	case <-ctx.Done():
		return
	}
	select {
	case err := <-result:
		if err != nil {
			panic("could not stop reflector: " + err.Error())
		}
	case <-ctx.Done():
		return
	}
}

func (r *ReflectorCtl) assertNotClosed() {
	if r.closed.IsSet() {
		panic("reflectorCtl has been closed")
	}
}

// lifecycle is run async by one goroutine and provides serial
// access to starting and stopping the reflector. This allows
// us to use local variables to control the state and use
// chans as the synchronization primitive
func (r *ReflectorCtl) lifecycle(appCtx context.Context) {
	var (
		running bool
		ctx     context.Context
		cancel  context.CancelFunc
		wg      sync.WaitGroup
	)
	for {
		select {
		case <-appCtx.Done():
			// the application is quitting
			events.Log("reflectorCtl stopping due to context err=%v", appCtx.Err())
			return
		case msg := <-r.messages:
			// another goroutine has asked for the reflector to either
			// be started or stopped.
			switch msg.desired {
			case true:
				if !running {
					ctx, cancel = context.WithCancel(appCtx)
					wg.Add(1)
					go func() {
						defer wg.Done()
						r.reflector.Start(ctx)
					}()
					running = true
				}
				msg.sendErr(appCtx, nil)
			case false:
				if running {
					cancel()
					done := make(chan struct{})
					go func() {
						defer close(done)
						wg.Wait()
					}()
					select {
					case <-done:
						running = false
					case <-time.After(reflectorCtlTimeout):
						errs.Incr("reflector-ctl-timeouts", stats.Tag{Name: "op", Value: "stop-reflector"})
						err := errors.Errorf("could not stop reflector after %s", reflectorCtlTimeout)
						msg.sendErr(appCtx, err)
						continue
					}
				}
				msg.sendErr(appCtx, nil)
			}
		}
	}
}

func (r *ReflectorCtl) Close() error {
	defer r.closed.SetTrue()
	ctx, cancel := context.WithTimeout(context.Background(), reflectorCtlTimeout)
	defer cancel()
	r.Stop(ctx)
	return r.reflector.Close()
}

func (m *reflectorCtlMsg) sendErr(ctx context.Context, err error) {
	select {
	case <-ctx.Done():
	case m.result <- err:
	case <-time.After(reflectorCtlTimeout):
		// should never happen but we don't want to block indefinitely
		// if someone did not create a result chan
		errs.Incr("reflector-ctl-timeouts", stats.Tag{Name: "op", Value: "send-err"})
		panic(errors.Errorf("could not send err on ctl msg after %s", reflectorCtlTimeout))
	}
}
