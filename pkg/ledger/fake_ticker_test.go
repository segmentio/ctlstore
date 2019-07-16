package ledger_test

import (
	"context"
	"testing"

	"github.com/segmentio/ctlstore/pkg/ledger"
	"github.com/stretchr/testify/require"
)

func TestFakeTicker(t *testing.T) {
	ft := ledger.NewFakeTicker()
	go func() {
		defer ft.Stop()
		ft.Tick(context.Background())
		ft.Tick(context.Background())
	}()
	count := 0
	for range ft.Ticker.C {
		count++
	}
	require.Equal(t, 2, count)
}
