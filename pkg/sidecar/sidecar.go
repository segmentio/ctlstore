package sidecar

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"github.com/segmentio/ctlstore"
	"github.com/segmentio/errors-go"
	"github.com/segmentio/stats"
	"github.com/segmentio/stats/httpstats"
)

type (
	Sidecar struct {
		bindAddr string
		reader   Reader
		maxRows  int
		handler  http.Handler
	}
	Config struct {
		BindAddr    string
		Reader      Reader
		MaxRows     int
		Application string
	}
	Reader interface {
		GetRowByKey(ctx context.Context, out interface{}, familyName string, tableName string, key ...interface{}) (found bool, err error)
		GetRowsByKeyPrefix(ctx context.Context, familyName string, tableName string, key ...interface{}) (*ctlstore.Rows, error)
		GetLedgerLatency(ctx context.Context) (time.Duration, error)
	}
	ReadRequest struct {
		Key []Key
	}
	// Key represents a primary key segment.  The 'Value' field should be used unless the key segment is a
	// varbinary field.  This is so that the json unmarshaling will decode base64 for the Binary property.
	Key struct {
		Value  interface{}
		Binary []byte
	}
)

func (k Key) ToValue() interface{} {
	switch {
	case k.Binary != nil:
		return k.Binary
	default:
		return k.Value
	}
}

func keysToInterface(keys []Key) []interface{} {
	var res []interface{}
	for _, k := range keys {
		res = append(res, k.ToValue())
	}
	return res
}

func New(config Config) (*Sidecar, error) {
	sidecar := &Sidecar{
		bindAddr: config.BindAddr,
		reader:   config.Reader,
		maxRows:  config.MaxRows,
	}
	mux := mux.NewRouter()
	handleErr := func(fn func(http.ResponseWriter, *http.Request) error) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			err := fn(w, r)
			switch {
			case err == nil:
			case errors.Is("limit-exceeded", err):
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			default:
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	}
	mux.HandleFunc("/get-row-by-key/{familyName}/{tableName}", handleErr(sidecar.getRowByKey)).Methods("POST")
	mux.HandleFunc("/get-rows-by-key-prefix/{familyName}/{tableName}", handleErr(sidecar.getRowsByKeyPrefix)).Methods("POST")
	mux.HandleFunc("/get-ledger-latency", handleErr(sidecar.getLedgerLatency)).Methods("GET")
	mux.HandleFunc("/healthcheck", handleErr(sidecar.healthcheck)).Methods("GET")
	mux.HandleFunc("/ping", handleErr(sidecar.ping)).Methods("GET")

	application := orUnknown(config.Application)
	stats.DefaultEngine.Tags = append(stats.DefaultEngine.Tags, stats.T("application", application))
	stats.DefaultEngine.Tags = stats.SortTags(stats.DefaultEngine.Tags) // tags must be sorted

	sidecar.handler = sidecar.statsHandler(mux)

	return sidecar, nil
}

func (s *Sidecar) Start(ctx context.Context) error {
	srv := &http.Server{
		Addr:         s.bindAddr,
		Handler:      s,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		ErrorLog:     log.New(os.Stderr, "SRV ERR:", log.LstdFlags),
	}
	defer srv.Close()
	err := srv.ListenAndServe()
	return errors.Wrap(err, "listen and serve")
}

func (s *Sidecar) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *Sidecar) statsHandler(delegate http.Handler) http.Handler {
	return httpstats.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ua := orUnknown(r.UserAgent())
		stats.Incr("requests-by-user-agent", stats.T("user-agent", ua))
		delegate.ServeHTTP(w, r)
	}))
}

func (s *Sidecar) getLedgerLatency(w http.ResponseWriter, r *http.Request) error {
	duration, err := s.reader.GetLedgerLatency(r.Context())
	if err != nil {
		return errors.Wrap(err, "get ledger latency")
	}
	res := map[string]interface{}{
		"value": duration.Seconds(),
		"unit":  "seconds",
	}
	return json.NewEncoder(w).Encode(res)
}

func (s *Sidecar) healthcheck(w http.ResponseWriter, r *http.Request) error {
	_, err := s.reader.GetLedgerLatency(r.Context())
	return errors.Wrap(err, "healthcheck")
}

func (s *Sidecar) ping(w http.ResponseWriter, r *http.Request) error {
	// for now, just hit the healthcheck. we can change this later.
	return s.healthcheck(w, r)
}

func (s *Sidecar) getRowsByKeyPrefix(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	family := vars["familyName"]
	table := vars["tableName"]

	var rr ReadRequest
	err := json.NewDecoder(r.Body).Decode(&rr)
	if err != nil {
		return errors.Wrap(err, "decode body")
	}
	res := make([]interface{}, 0)
	rows, err := s.reader.GetRowsByKeyPrefix(r.Context(), family, table, keysToInterface(rr.Key)...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		out := make(map[string]interface{})
		err = rows.Scan(out)
		if err != nil {
			return errors.Wrap(err, "scan")
		}
		res = append(res, out)
		if s.maxRows > 0 && len(res) > s.maxRows {
			err = errors.Errorf("max row count (%d) exceeded", s.maxRows)
			err = errors.WithTypes(err, "limit-exceeded")
			return err
		}
	}
	err = rows.Err()
	if err != nil {
		return err
	}
	stats.Observe("get-rows-by-key-prefix-num-rows", len(res), stats.T("family", family), stats.T("table", table))
	err = json.NewEncoder(w).Encode(res)
	return err
}

func (s *Sidecar) getRowByKey(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	family := vars["familyName"]
	table := vars["tableName"]

	var rr ReadRequest
	err := json.NewDecoder(r.Body).Decode(&rr)
	if err != nil {
		return errors.Wrap(err, "decode body")
	}

	out := make(map[string]interface{})
	found, err := s.reader.GetRowByKey(r.Context(), out, family, table, keysToInterface(rr.Key)...)
	if err != nil {
		return err
	}
	if !found {
		w.Header().Set("X-Ctlstore", "Not Found") // to differentiate between route based 404s
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
	err = json.NewEncoder(w).Encode(out)
	return err
}

func orUnknown(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}
