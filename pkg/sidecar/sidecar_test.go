package sidecar

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/segmentio/ctlstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerLatency(t *testing.T) {
	tu, teardown := ctlstore.NewLDBTestUtil(t)
	defer teardown()

	// necessary to have at least one LDB table to pass healthcheck
	tu.CreateTable(ctlstore.LDBTestTableDef{
		Family: "family",
		Name:   "table",
		Fields: [][]string{
			{"key", "string"},
		},
		KeyFields: []string{"key"},
		Rows: [][]interface{}{
			{"key-1"},
		},
	})

	sc, err := New(Config{
		Reader:  ctlstore.NewLDBReaderFromDB(tu.DB),
		MaxRows: 0,
	})
	require.NoError(t, err)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/get-ledger-latency", nil)
	sc.ServeHTTP(w, r)
	require.EqualValues(t, http.StatusOK, w.Code, w.Body.String())
	var res map[string]interface{}
	err = json.Unmarshal(w.Body.Bytes(), &res)
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
	require.Equal(t, "seconds", res["unit"])
	seconds, ok := res["value"].(float64)
	require.True(t, ok, "value not an float64. it is instead a %T", res["value"])
	require.True(t, seconds >= 0 && seconds <= 5, "weird seconds value: %v", seconds)
}

func TestHealthcheck(t *testing.T) {
	tu, teardown := ctlstore.NewLDBTestUtil(t)
	defer teardown()

	// necessary to have at least one LDB table to pass healthcheck
	tu.CreateTable(ctlstore.LDBTestTableDef{
		Family: "family",
		Name:   "table",
		Fields: [][]string{
			{"key", "string"},
		},
		KeyFields: []string{"key"},
		Rows: [][]interface{}{
			{"key-1"},
		},
	})

	sc, err := New(Config{
		Reader:  ctlstore.NewLDBReaderFromDB(tu.DB),
		MaxRows: 0,
	})
	require.NoError(t, err)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/healthcheck", nil)
	sc.ServeHTTP(w, r)
	require.EqualValues(t, http.StatusOK, w.Code, w.Body.String())
}

func TestPing(t *testing.T) {
	tu, teardown := ctlstore.NewLDBTestUtil(t)
	defer teardown()

	// necessary to have at least one LDB table to pass healthcheck
	tu.CreateTable(ctlstore.LDBTestTableDef{
		Family: "family",
		Name:   "table",
		Fields: [][]string{
			{"key", "string"},
		},
		KeyFields: []string{"key"},
		Rows: [][]interface{}{
			{"key-1"},
		},
	})

	sc, err := New(Config{
		Reader:  ctlstore.NewLDBReaderFromDB(tu.DB),
		MaxRows: 0,
	})
	require.NoError(t, err)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ping", nil)
	sc.ServeHTTP(w, r)
	require.EqualValues(t, http.StatusOK, w.Code, w.Body.String())
}

func TestFetchCtlstoreData(t *testing.T) {
	for _, test := range []struct {
		name        string
		family      string
		table       string
		useMulti    bool
		rr          ReadRequest
		status      int
		respHeaders map[string]string
		result      interface{}
	}{
		{
			name:   "row found",
			family: "test_family",
			table:  "test_table",
			rr:     ReadRequest{[]Key{{Value: "test-key"}}},
			status: http.StatusOK,
			result: map[string]interface{}{
				"key":   "test-key",
				"value": "test-value",
			},
		},
		{
			name:     "rows found",
			family:   "test_family",
			table:    "test_table",
			useMulti: true,
			rr:       ReadRequest{[]Key{}},
			status:   http.StatusOK,
			result: []interface{}{
				map[string]interface{}{
					"key":   "test-key",
					"value": "test-value",
				},
				map[string]interface{}{
					"key":   "test-key-2",
					"value": "test-value-2",
				},
			},
		},
		{
			name:     "rows not found",
			family:   "test_family",
			table:    "test_table",
			rr:       ReadRequest{[]Key{{Value: "does not exist"}}},
			useMulti: true,
			status:   http.StatusOK,
			result:   []interface{}{},
		},
		{
			name:   "row not found",
			family: "test_family",
			table:  "test_table",
			rr:     ReadRequest{[]Key{{Value: "does not exist"}}},
			status: http.StatusNotFound,
			result: nil,
			respHeaders: map[string]string{
				"X-Ctlstore": "Not Found",
			},
		},
		{
			name:   "row found, binary key",
			family: "test_family",
			table:  "binary_key_table",
			rr:     ReadRequest{[]Key{{Binary: []byte{0xde, 0xad, 0xbe, 0xef}}}},
			status: http.StatusOK,
			result: map[string]interface{}{
				"key":   base64.StdEncoding.EncodeToString([]byte{0xde, 0xad, 0xbe, 0xef}),
				"value": "test-value",
			},
		},
		{
			name:     "rows found, binary key",
			family:   "test_family",
			table:    "binary_key_table",
			useMulti: true,
			rr:       ReadRequest{[]Key{{Binary: []byte{0xde, 0xad, 0xbe, 0xef}}}},
			status:   http.StatusOK,
			result: []interface{}{
				map[string]interface{}{
					"key":   base64.StdEncoding.EncodeToString([]byte{0xde, 0xad, 0xbe, 0xef}),
					"value": "test-value",
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tu, teardown := ctlstore.NewLDBTestUtil(t)
			defer teardown()
			tu.CreateTable(ctlstore.LDBTestTableDef{
				Family: "test_family",
				Name:   "test_table",
				Fields: [][]string{
					{"key", "string"},
					{"value", "string"},
				},
				KeyFields: []string{"key"},
				Rows: [][]interface{}{
					{"test-key", "test-value"},
					{"test-key-2", "test-value-2"},
				},
			})
			tu.CreateTable(ctlstore.LDBTestTableDef{
				Family: "test_family",
				Name:   "binary_key_table",
				Fields: [][]string{
					{"key", "bytestring"},
					{"value", "string"},
				},
				KeyFields: []string{"key"},
				Rows: [][]interface{}{
					{[]byte{0xde, 0xad, 0xbe, 0xef}, "test-value"},
				},
			})
			sc, err := New(Config{
				Reader:  ctlstore.NewLDBReaderFromDB(tu.DB),
				MaxRows: 0,
			})
			require.NoError(t, err)
			keys, err := json.Marshal(test.rr)
			require.NoError(t, err)
			w := httptest.NewRecorder()
			urlPrefix := "/get-row-by-key"
			if test.useMulti {
				urlPrefix = "/get-rows-by-key-prefix"
			}
			r := httptest.NewRequest(http.MethodPost, filepath.Join(urlPrefix, test.family, test.table),
				bytes.NewReader(keys))

			sc.ServeHTTP(w, r)

			require.EqualValues(t, test.status, w.Code, w.Body.String())

			if test.result != nil {
				var res interface{}
				err = json.Unmarshal(w.Body.Bytes(), &res)
				require.NoError(t, err)
				require.EqualValues(t, test.result, res)
			} else {
				require.EqualValues(t, 0, len(w.Body.Bytes()), w.Body.String())
			}

			for k, v := range test.respHeaders {
				require.Equal(t, v, w.Header().Get(k))
			}
		})
	}

}
