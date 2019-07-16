package utils

import (
	"context"
	"time"
)

// CtxLoop blocks and fires the callback function on a tick.
func CtxLoop(ctx context.Context, delay time.Duration, fn func()) {
	ticker := time.NewTicker(delay)
	defer ticker.Stop()
	CtxLoopTicker(ctx, ticker, fn)
}

// CtxLoopTicker blocks and fires the callback function on a tick.
func CtxLoopTicker(ctx context.Context, ticker *time.Ticker, fn func()) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn()
		}
	}
}

// CtxFireLoop blocks and fires the callback function on a tick. The callback
// function is fired first before the first delay.
func CtxFireLoop(ctx context.Context, delay time.Duration, fn func()) {
	ticker := time.NewTicker(delay)
	defer ticker.Stop()
	CtxFireLoopTicker(ctx, ticker, fn)
}

// CtxFireLoopTicker blocks and fires the callback function on a tick. The callback
// function is fired first before the first delay.
func CtxFireLoopTicker(ctx context.Context, ticker *time.Ticker, fn func()) {
	fn()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn()
		}
	}
}
