package event

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const numEvents = 5

	changelog := &fakeChangelog{}
	for i := 0; i < numEvents; i++ {
		changelog.ers = append(changelog.ers, eventErr{
			event: Event{Sequence: int64(i)},
		})
	}
	iter, err := NewIterator(ctx, "test file", func(i *Iterator) {
		i.changelog = changelog
	})
	require.NoError(t, err)
	defer func() {
		err := iter.Close()
		require.NoError(t, err)
	}()
	for i := 0; i < numEvents; i++ {
		event, err := iter.Next(ctx)
		require.NoError(t, err)
		require.EqualValues(t, i, event.Sequence)
	}
}

func TestIteratorFailedChangelogStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	iter, err := NewIterator(ctx, "test file", func(i *Iterator) {
		i.changelog = &fakeChangelog{
			startErr: errors.New("failure"), // force a failure on startup
			ers: []eventErr{
				{event: Event{Sequence: 0}},
				{event: Event{Sequence: 3}},
				{event: Event{Sequence: 4}},
			},
		}
	})
	require.Nil(t, iter)
	require.EqualError(t, err, "start changelog: failure")

}

func TestIteratorSkippedEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	iter, err := NewIterator(ctx, "test file", func(i *Iterator) {
		i.changelog = &fakeChangelog{
			ers: []eventErr{
				{event: Event{Sequence: 0}},
				{event: Event{Sequence: 3}},
				{event: Event{Sequence: 4}},
			},
		}
	})
	require.NoError(t, err)
	defer func() {
		err := iter.Close()
		require.NoError(t, err)
	}()
	event, err := iter.Next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 0, event.Sequence)

	event, err = iter.Next(ctx)
	require.EqualValues(t, 3, event.Sequence)
	require.EqualError(t, err, "out of sync with changelog. invalidate caches please.")

	event, err = iter.Next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 4, event.Sequence)

}
