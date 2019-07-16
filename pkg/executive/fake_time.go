package executive

import (
	"sync/atomic"
	"time"
)

// fakeTime is a type that produces time.Times to second precision
type fakeTime struct {
	epoch int64
}

func newFakeTime(epoch int64) *fakeTime {
	ft := &fakeTime{}
	ft.set(epoch)
	return ft
}

func (t *fakeTime) set(epoch int64) {
	atomic.StoreInt64(&t.epoch, epoch)
}

func (t *fakeTime) get() time.Time {
	val := atomic.LoadInt64(&t.epoch)
	return time.Unix(val, 0)
}

func (t *fakeTime) add(delta int64) {
	atomic.AddInt64(&t.epoch, delta)
}
