package fakes

import (
	"context"

	"github.com/segmentio/ctlstore/pkg/utils"
)

type (
	FakeReflector struct {
		Running utils.AtomicBool
		Closed  utils.AtomicBool
		Events  chan string
	}
)

func NewFakeReflector() *FakeReflector {
	return &FakeReflector{
		Events: make(chan string, 1024),
	}
}

func (r *FakeReflector) NextEvent(ctx context.Context) string {
	select {
	case event := <-r.Events:
		return event
	case <-ctx.Done():
		panic(ctx.Err())
	}
}

func (r *FakeReflector) Start(ctx context.Context) error {
	r.Running.SetTrue()
	r.SendEvent("started")
	<-ctx.Done()
	r.Running.SetFalse()
	r.SendEvent("stopped")
	return ctx.Err()
}

func (r *FakeReflector) Stop() {

}

func (r *FakeReflector) Close() error {
	r.Closed.SetTrue()
	r.SendEvent("closed")
	return nil
}

func (r *FakeReflector) SendEvent(name string) {
	select {
	case r.Events <- name:
	default:
		panic("event chan full")
	}
}
