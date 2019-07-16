package event

import (
	"context"

	"github.com/pkg/errors"
)

type (
	fakeChangelog struct {
		startErr error
		ers      []eventErr
		pos      int
	}
)

func (c *fakeChangelog) start(ctx context.Context) error {
	return c.startErr
}

func (c *fakeChangelog) next(ctx context.Context) (Event, error) {
	if c.pos >= len(c.ers) {
		return Event{}, errors.New("exhausted changelog set")
	}
	ee := c.ers[c.pos]
	c.pos++
	return ee.event, ee.err
}
