package sqlite

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChangeBuffer(t *testing.T) {
	var buf SQLChangeBuffer
	assert.Len(t, buf.Pop(), 0)

	buf.Add(SQLiteWatchChange{
		DatabaseName: "t1",
	})
	buf.Add(SQLiteWatchChange{
		DatabaseName: "t2",
	})
	pop := buf.Pop()
	assert.EqualValues(t, []SQLiteWatchChange{
		{DatabaseName: "t1"},
		{DatabaseName: "t2"},
	}, pop)

	// verify there are no more changes
	assert.Len(t, buf.Pop(), 0)

}
