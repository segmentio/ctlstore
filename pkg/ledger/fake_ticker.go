package ledger

import (
	"context"
	"time"
)

// FakeTicker allows us to manually control when a send happens
// on the channel.  The Ticker property allows us to adhere to
// the *time.Ticker interface.
type FakeTicker struct {
	Ticker *time.Ticker
	ch     chan time.Time
}

func NewFakeTicker() *FakeTicker {
	ch := make(chan time.Time)
	return &FakeTicker{
		ch: ch,
		Ticker: &time.Ticker{
			C: ch,
		},
	}
}

func (f *FakeTicker) Tick(ctx context.Context) {
	select {
	case f.ch <- time.Now():
	case <-ctx.Done():
	}
}

func (f *FakeTicker) Stop() {
	close(f.ch)
}
