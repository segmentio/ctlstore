package executive

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/units"
	"github.com/stretchr/testify/require"
)

func TestDBLimiter(t *testing.T) {
	withDBTypes(t, func(dbType string) {
		u := newDbExecTestUtil(t, dbType)
		ctldb := u.db

		ctx, cancel := context.WithCancel(u.ctx)
		defer cancel()

		const (
			familyName     = "db_limiter_family"
			tableName      = "db_limiter_table"
			bucketInterval = 5 * time.Second
			writerLimit    = 5 // per bucket interval
			writerName     = "db-limiter-writer-name"
			writerSecret   = "db-limiter-writer-secret"
		)

		// we control the time using a fakeTime with an epoch of 1000s
		fakeTime := newFakeTime(1000)
		defaultTableLimit := limits.SizeLimits{MaxSize: 30 * units.KILOBYTE, WarnSize: 20 * units.KILOBYTE}
		limiter := newDBLimiter(ctldb, dbType, defaultTableLimit, bucketInterval, writerLimit)
		limiter.timeFunc = fakeTime.get
		require.NoError(t, limiter.tableSizer.refresh(ctx))
		require.NoError(t, u.e.CreateFamily(familyName))
		executive := &executiveService{ctldb: u.db, ctx: ctx, limiter: limiter, serveTimeout: 10 * time.Second}

		fieldNames := []string{"name", "data"}
		fieldTypes := []schema.FieldType{schema.FTString, schema.FTBinary}
		keyFields := []string{"name"}
		require.NoError(t, u.e.CreateTable(familyName, tableName, fieldNames, fieldTypes, keyFields))
		require.NoError(t, u.e.RegisterWriter(writerName, writerSecret))

		payloadFunc := newMutationPayload(t, tableName, "test", int(10*units.KILOBYTE))

		// makeMutation is a func that performs a mutation supplied by the payloadFunc. it fails the test if it
		// fails, so no need to return a value.
		makeMutation := func(expectedCode int) {
			req := httptest.NewRequest("POST", "/families/"+familyName+"/mutations", payloadFunc())
			req.Header.Set("ctlstore-writer", writerName)
			req.Header.Set("ctlstore-secret", writerSecret)
			w := httptest.NewRecorder()
			executive.ServeHTTP(w, req)
			resp := w.Result()
			defer resp.Body.Close()
			if expectedCode != resp.StatusCode {
				b, _ := ioutil.ReadAll(resp.Body)
				require.Failf(t, "request failed", "Expected %d, got %d: %s", expectedCode, resp.StatusCode, b)
			}
		}

		// do the first mutation to create the table
		makeMutation(http.StatusOK)
		// let the table sizer find the table so it stops logging about it
		require.NoError(t, limiter.tableSizer.refresh(ctx))

		// at this point, we're at epoch 1000 and we've written 1 row. our limit is 5 per period, so let's
		// fill out the rest of our quota
		for i := 0; i < 4; i++ {
			makeMutation(http.StatusOK)
		}

		// if we do another mutation it should fail
		makeMutation(http.StatusTooManyRequests)

		// shift the epoch up by $period
		fakeTime.add(int64(bucketInterval / time.Second))

		// should then be able to store 5 more but no more than that
		for i := 0; i < 5; i++ {
			makeMutation(http.StatusOK)
		}
		makeMutation(http.StatusTooManyRequests)

		// leaving the epoch where it is, let's add a per-writer override
		_, err := u.db.ExecContext(ctx, "insert into max_writer_rates (writer_name, max_rows_per_minute) values(?,?)",
			writerName, 120) // gets converted from 120/min -> 10/5s
		require.NoError(t, err)
		require.NoError(t, limiter.refreshWriterLimits(ctx))

		// we should be able to make five writes now before it fails
		for i := 0; i < 5; i++ {
			makeMutation(http.StatusOK)
		}
		makeMutation(http.StatusTooManyRequests)

		// now with the override still in place, bump the epoch and observe that writes go through
		// since we're in a new bucket
		fakeTime.add(int64(bucketInterval / time.Second))
		makeMutation(http.StatusOK)

		// finally, refresh the table sizer. this will pick up the new limits and should deny the
		// request (if on mysql)
		require.NoError(t, limiter.tableSizer.refresh(ctx))
		if dbType == "sqlite3" {
			makeMutation(http.StatusOK)
		} else {
			makeMutation(http.StatusInsufficientStorage)
		}

		countRows := func() int64 {
			row := u.db.QueryRowContext(ctx, "select count(*) from writer_usage")
			var count int64
			err = row.Scan(&count)
			require.NoError(t, err)
			return count
		}

		// verify we have rows in the writer_usage table
		count := countRows()
		require.True(t, count > 0, "unexpected count: %d", count)

		// verify that the cleaner leaves our rows alone since we are still at the same epoch
		err = limiter.deleteOldUsageData(ctx)
		require.NoError(t, err)
		require.EqualValues(t, count, countRows())

		// set the time to now so that we can trigger the cleaner to clean up our old rows
		fakeTime.set(time.Now().Unix())

		// perform a cleanup
		err = limiter.deleteOldUsageData(ctx)
		require.NoError(t, err)

		// verify no more
		count = countRows()
		require.EqualValues(t, 0, count)
	})
}

// newMutationPayload is a helper that produces a func that produces a reader that supplies a
// payload to the mutation api. it maintains its own internal cookie that gets incremented on
// each payload
func newMutationPayload(t *testing.T, table string, name string, length int) func() io.Reader {
	var mutationSeq uint64
	return func() io.Reader {
		seq := atomic.AddUint64(&mutationSeq, 1)
		data := make([]byte, length)
		rand.Read(data)
		type mutation struct {
			TableName string                 `json:"table"`
			Delete    bool                   `json:"delete"`
			Values    map[string]interface{} `json:"values"`
		}
		type payload struct {
			Cookie    []byte     `json:"cookie"`
			Mutations []mutation `json:"mutations"`
		}
		cookie := make([]byte, 8)
		binary.BigEndian.PutUint64(cookie, seq)
		p := payload{
			Cookie: cookie,
			Mutations: []mutation{
				{
					TableName: table,
					Values: map[string]interface{}{
						"name": fmt.Sprintf("%s-%d", name, mutationSeq),
						"data": data,
					},
				},
			},
		}
		b, err := json.Marshal(p)
		require.NoError(t, err)
		return bytes.NewReader(b)
	}
}

func TestLimiterPeriodEpoch(t *testing.T) {
	tf := func() time.Time { return time.Unix(1000, 0) }
	limiter := dbLimiter{timeFunc: tf, defaultWriterLimit: limits.RateLimit{Period: time.Minute, Amount: 100}}

	require.EqualValues(t, 960, limiter.periodEpoch())

	limiter.defaultWriterLimit.Period = 5 * time.Second
	require.EqualValues(t, 1000, limiter.periodEpoch())

	limiter.timeFunc = func() time.Time { return time.Unix(1001, 0) }
	require.EqualValues(t, 1000, limiter.periodEpoch())

	limiter.timeFunc = func() time.Time { return time.Unix(1005, 0) }
	require.EqualValues(t, 1005, limiter.periodEpoch())
}
