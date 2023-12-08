package event

import (
	"context"
	"github.com/segmentio/events/v2"
	"strings"

	"github.com/segmentio/errors-go"
)

type (
	changelog interface {
		start(ctx context.Context) error
		next(ctx context.Context) (Event, error)
	}
	Iterator struct {
		changelog  changelog          // streams in events from somewhere
		cancelFunc context.CancelFunc // used to shut down the changelog
		previous   *Event             // the previous event we read
	}
	IteratorOpt      func(i *Iterator)
	FilteredIterator struct {
		iterator *Iterator
		family   string
		table    string
	}
)

var (
	ErrOutOfSync = errors.New("out of sync with changelog. invalidate caches please.")
)

// NewIterator returns a new iterator that looks for changes in the background and
// then exposes those changes through the Next method.  Make sure to Close() the
// iterator when you are done using it.
//
// If ErrOutOfSync is returned, that means that the iterator likely could not keep
// up with the changelog. Please invalidate any caches dependent on this iterator.
//
// If a different error is returned, it's not really known at this time the best way
// to deal with it.  It's possible that it could be a change in the changelog json
// schema, or something more temporary.  Best response for now will be to log and instrument
// the error, and then just invalidate the cache the same way you would with ErrOutOfSync.
// As time goes on, we'll know a little bit better how to operate this under real-world
// conditions.
func NewIterator(ctx context.Context, changelogPath string, opts ...IteratorOpt) (*Iterator, error) {
	iter := &Iterator{}
	for _, opt := range opts {
		opt(iter)
	}
	if iter.changelog == nil {
		cl := newFileChangelog(changelogPath)
		if err := cl.validate(); err != nil {
			return nil, errors.Wrap(err, "validate changelog")
		}
		iter.changelog = cl
	}
	ctx, iter.cancelFunc = context.WithCancel(ctx)
	if err := iter.changelog.start(ctx); err != nil {
		return nil, errors.Wrap(err, "start changelog")
	}
	return iter, nil
}

// Next blocks and returns the next event
func (i *Iterator) Next(ctx context.Context) (event Event, err error) {
	event, err = i.changelog.next(ctx)
	if err != nil {
		return event, err
	}
	previous := i.previous
	i.previous = &event
	if previous != nil {
		if previous.Sequence != event.Sequence-1 {
			events.Log("out of sync sequences (cur-1 should equal prev), prev: %d cur: %d", previous.Sequence, event.Sequence)
			// we have an out of order changelog
			return event, ErrOutOfSync
		}
	}
	return event, err
}

func (i *Iterator) Close() error {
	i.cancelFunc() // shut down the changelog
	return nil
}

// NewFilteredIterator returns a new iterator that looks for changes to the specified family
// and table in the background and then exposes those changes through the Next method.
// Make sure to Close() the iterator when you are done using it.
//
// If ErrOutOfSync is returned, that means that the iterator likely could not keep
// up with the changelog. Please invalidate any caches dependent on this iterator.
//
// If a different error is returned, it's not really known at this time the best way
// to deal with it.  It's possible that it could be a change in the changelog json
// schema, or something more temporary.  Best response for now will be to log and instrument
// the error, and then just invalidate the cache the same way you would with ErrOutOfSync.
// As time goes on, we'll know a little bit better how to operate this under real-world
// conditions.
func NewFilteredIterator(ctx context.Context, changelogPath, family, table string, opts ...IteratorOpt) (*FilteredIterator, error) {
	iter, err := NewIterator(ctx, changelogPath, opts...)
	if err != nil {
		return nil, err
	}
	fi := &FilteredIterator{
		iterator: iter,
		family:   family,
		table:    table,
	}
	return fi, nil
}

// Next blocks and returns the next event that matches the specified family and table
func (i *FilteredIterator) Next(ctx context.Context) (event Event, err error) {
	for {
		event, err = i.iterator.Next(ctx)
		if err != nil ||
			(strings.EqualFold(event.RowUpdate.FamilyName, i.family) && strings.EqualFold(event.RowUpdate.TableName, i.table)) {
			break
		}
	}
	return event, err
}

func (i *FilteredIterator) Close() error {
	return i.iterator.Close()
}
