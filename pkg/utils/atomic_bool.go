package utils

import (
	"sync/atomic"
)

type AtomicBool int32

func (b *AtomicBool) IsSet() bool {
	return atomic.LoadInt32((*int32)(b)) != 0
}

func (b *AtomicBool) SetTrue() {
	atomic.StoreInt32((*int32)(b), 1)
}

func (b *AtomicBool) SetFalse() {
	atomic.StoreInt32((*int32)(b), 0)
}
