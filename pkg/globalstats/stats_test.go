package globalstats

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/stats/v4"
	"github.com/stretchr/testify/require"
)

type fakeHandler struct {
	mut            sync.Mutex
	measuresByName map[string][]stats.Measure
}

// fakeHandler needs to conform to the stats.Handler interface.
var _ stats.Handler = &fakeHandler{}

func newFakeHandler() *fakeHandler {
	return &fakeHandler{
		measuresByName: make(map[string][]stats.Measure),
	}
}

func (h *fakeHandler) HandleMeasures(t time.Time, measures ...stats.Measure) {
	h.mut.Lock()
	defer h.mut.Unlock()

	for _, m := range measures {
		if _, ok := h.measuresByName[m.Name]; !ok {
			h.measuresByName[m.Name] = []stats.Measure{}
		}
		h.measuresByName[m.Name] = append(h.measuresByName[m.Name], m.Clone())
	}
}

func TestGlobalStats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Overwrite the default engine with a testing mock.
	h := newFakeHandler()
	originalHandler := stats.DefaultEngine.Handler
	stats.DefaultEngine.Handler = h
	defer func() {
		// Replace the default DefaultEngine.Handler
		stats.DefaultEngine.Handler = originalHandler
	}()

	// Initialize globalstats.
	Initialize(ctx, Config{
		FlushEvery: 10 * time.Millisecond,
	})

	// Perform some example Incr operations.
	Incr("a", "family-a", "table-a")
	Incr("b", "family-a", "table-a")
	Incr("a", "family-a", "table-a")
	Incr("a", "family-a", "table-b")

	// Wait for the Incr operations to propogate to the flusher.
	time.Sleep(15 * time.Millisecond)
	cancel()

	h.mut.Lock()
	defer h.mut.Unlock()

	// Verify that the three Incr operations were flushed.
	flusherMeasures, ok := h.measuresByName["ctlstore.global"]
	require.True(t, ok)
	// Sort the measures we received so that we can reliably compare the output.
	sort.Slice(flusherMeasures, func(i, j int) bool {
		fi, fj := flusherMeasures[i], flusherMeasures[j]
		if len(fi.Fields) != len(fj.Fields) || len(fi.Fields) == 0 {
			return len(fi.Fields) < len(fj.Fields)
		}
		if fi.Fields[0].Value.Int() != fj.Fields[0].Value.Int() {
			return fi.Fields[0].Value.Int() < fj.Fields[0].Value.Int()
		}
		return fi.Fields[0].Name < fj.Fields[0].Name
	})
	fmt.Printf("%+v\n", flusherMeasures)
	require.Equal(t, []stats.Measure{
		{
			Name: "ctlstore.global",
			Fields: []stats.Field{
				stats.MakeField("dropped-stats", 0, stats.Counter),
			},
			Tags: []stats.Tag{
				stats.T("app", "globalstats.test"),
				stats.T("version", "unknown"),
			},
		},
		{
			Name: "ctlstore.global",
			Fields: []stats.Field{
				stats.MakeField("a", 1, stats.Counter),
			},
			Tags: []stats.Tag{
				stats.T("app", "globalstats.test"),
				stats.T("family", "family-a"),
				stats.T("table", "table-b"),
				stats.T("version", "unknown"),
			},
		},
		{
			Name: "ctlstore.global",
			Fields: []stats.Field{
				stats.MakeField("b", 1, stats.Counter),
			},
			Tags: []stats.Tag{
				stats.T("app", "globalstats.test"),
				stats.T("family", "family-a"),
				stats.T("table", "table-a"),
				stats.T("version", "unknown"),
			},
		},
		{
			Name: "ctlstore.global",
			Fields: []stats.Field{
				stats.MakeField("a", 2, stats.Counter),
			},
			Tags: []stats.Tag{
				stats.T("app", "globalstats.test"),
				stats.T("family", "family-a"),
				stats.T("table", "table-a"),
				stats.T("version", "unknown"),
			},
		},
	}, flusherMeasures)
}
