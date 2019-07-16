package heartbeat

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestHeartbeat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	rCh := make(chan *http.Request)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("Received", r.URL.Path)
		switch r.URL.Path {
		case "/writers/writer-name":
		case "/families/my-family":
		case "/families/my-family/tables/my-table":
		case "/families/my-family/mutations":
		default:
			http.Error(w, fmt.Sprintf("invalid path: %s", r.URL.Path), http.StatusInternalServerError)
			t.Fatal("unexpected:", r.URL)
		}
		select {
		case rCh <- r:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}))
	defer server.Close()
	go func() {
		h, err := HeartbeatFromConfig(HeartbeatConfig{
			Table:             "my-table",
			Family:            "my-family",
			ExecutiveURL:      server.URL,
			WriterName:        "writer-name",
			WriterSecret:      "writer-secret",
			HeartbeatInterval: 10 * time.Hour,
		})
		require.NoError(t, err)
		require.NotNil(t, h)
		defer h.Close()

		h.Start(ctx)
	}()

	nextRequest := func() *http.Request {
		select {
		case r := <-rCh:
			return r
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		panic("unreachable")
	}
	r := nextRequest()
	require.Equal(t, http.MethodPost, r.Method)
	require.Equal(t, "/writers/writer-name", r.URL.Path)

	r = nextRequest()
	require.Equal(t, http.MethodPost, r.Method)
	require.Equal(t, "/families/my-family", r.URL.Path)

	r = nextRequest()
	require.Equal(t, http.MethodPost, r.Method)
	require.Equal(t, "/families/my-family/tables/my-table", r.URL.Path)

	// verify at least one mutation was sent

	r = nextRequest()
	require.Equal(t, http.MethodPost, r.Method)
	require.Equal(t, "/families/my-family/mutations", r.URL.Path)
}
