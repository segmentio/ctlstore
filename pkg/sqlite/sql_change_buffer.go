package sqlite

import (
	"sync"
)

// SQLChangeBuffer accumulates sqliteWatchChanges and allows them to be popped
// off later when writing the changelog.
type SQLChangeBuffer struct {
	mut     sync.Mutex
	changes []SQLiteWatchChange
}

// add appends a change to the end of the buffer
func (b *SQLChangeBuffer) Add(change SQLiteWatchChange) {
	b.mut.Lock()
	defer b.mut.Unlock()
	b.changes = append(b.changes, change)
}

// pop returns the accumulated changes and then resets the buffer
func (b *SQLChangeBuffer) Pop() []SQLiteWatchChange {
	b.mut.Lock()
	defer b.mut.Unlock()
	res := b.changes
	b.changes = nil
	return res
}
