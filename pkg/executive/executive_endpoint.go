package executive

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/log"
	"github.com/segmentio/stats/v4"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// ExecutiveEndpoint is an HTTP 'wrapper' for ExecutiveInterface
type ExecutiveEndpoint struct {
	HealthChecker                  HealthChecker
	Exec                           ExecutiveInterface
	EnableDestructiveSchemaChanges bool
}

func (ee *ExecutiveEndpoint) handleFamilyRoute(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	// panic here means Mux is totally screwed, all bets are off!
	familyName := vars["familyName"]

	err := ee.Exec.CreateFamily(familyName)

	if err != nil {
		writeErrorResponse(err, w)
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}

func (ee *ExecutiveEndpoint) handleTablesRoute(w http.ResponseWriter, r *http.Request) {
	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(err, w)
		return
	}

	var payload []schema.Table
	err = json.Unmarshal(rawBody, &payload)
	if err != nil {
		writeErrorResponse(&errs.BadRequestError{Err: "JSON Error: " + err.Error()}, w)
		return
	}

	err = ee.Exec.CreateTables(payload)
	if err != nil {
		writeErrorResponse(err, w)
		return
	}
}

func (ee *ExecutiveEndpoint) handleTableRoute(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	// if these panic, Mux is broken and nothing is sacred anymore
	familyName := vars["familyName"]
	tableName := vars["tableName"]

	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(err, w)
		return
	}

	switch r.Method {
	case "POST":
		payload := struct {
			Fields    [][]string `json:"fields"`
			KeyFields []string   `json:"keyFields"`
		}{}

		err = json.Unmarshal(rawBody, &payload)
		if err != nil {
			writeErrorResponse(&errs.BadRequestError{Err: "JSON Error: " + err.Error()}, w)
			return
		}

		fieldNames, fieldTypes, err := schema.UnzipFieldsParam(payload.Fields)
		if err != nil {
			writeErrorResponse(&errs.BadRequestError{Err: "Error unzipping fields: " + err.Error()}, w)
			return
		}

		err = ee.Exec.CreateTable(familyName, tableName, fieldNames, fieldTypes, payload.KeyFields)
		if err != nil {
			writeErrorResponse(err, w)
			return
		}

	case "PUT":
		payload := struct {
			Fields [][]string `json:"fields"`
		}{}

		err = json.Unmarshal(rawBody, &payload)
		if err != nil {
			writeErrorResponse(&errs.BadRequestError{Err: "JSON Error: " + err.Error()}, w)
			return
		}

		fieldNames, fieldTypes, err := schema.UnzipFieldsParam(payload.Fields)
		if err != nil {
			writeErrorResponse(&errs.BadRequestError{Err: "Error unzipping fields: " + err.Error()}, w)
			return
		}

		err = ee.Exec.AddFields(familyName, tableName, fieldNames, fieldTypes)
		if err != nil {
			writeErrorResponse(err, w)
			return
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (ee *ExecutiveEndpoint) handleCookieRoute(w http.ResponseWriter, r *http.Request) {
	hdrWriter := r.Header.Get("ctlstore-writer")
	hdrSecret := r.Header.Get("ctlstore-secret")

	if r.Method == "GET" {
		cookie, err := ee.Exec.GetWriterCookie(hdrWriter, hdrSecret)
		if err != nil {
			writeErrorResponse(err, w)
			return
		}

		_, _ = w.Write(cookie)
		return
	}
	if r.Method == "POST" {
		rawBody, err := io.ReadAll(r.Body)
		if err != nil {
			writeErrorResponse(err, w)
			return
		}

		err = ee.Exec.SetWriterCookie(hdrWriter, hdrSecret, rawBody)
		if err != nil {
			writeErrorResponse(err, w)
		}

		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (ee *ExecutiveEndpoint) handleFamilySchemasRoute(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	familyName := vars["familyName"]
	schemas, err := ee.Exec.FamilySchemas(familyName)
	switch {
	case err == nil:
	default:
		writeErrorResponse(err, w)
		return
	}
	bs, err := json.Marshal(schemas)
	if err != nil {
		writeErrorResponse(err, w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

func (ee *ExecutiveEndpoint) handleTableSchemaRoute(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	familyName := vars["familyName"]
	tableName := vars["tableName"]
	schema, err := ee.Exec.TableSchema(familyName, tableName)
	switch {
	case err == nil:
		// do nothing, no error
	case errors.Cause(err) == ErrTableDoesNotExist:
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	default:
		writeErrorResponse(err, w)
		return
	}
	bs, err := json.Marshal(schema)
	if err != nil {
		writeErrorResponse(err, w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

func (ee *ExecutiveEndpoint) handleWritersRoute(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	if r.Method == "POST" {
		rawBody, err := io.ReadAll(r.Body)
		if err != nil {
			writeErrorResponse(err, w)
			return
		}

		secret := string(rawBody)
		err = ee.Exec.RegisterWriter(vars["writerName"], secret)
		if err != nil {
			writeErrorResponse(err, w)
			return
		}

		w.WriteHeader(http.StatusOK)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (ee *ExecutiveEndpoint) handleMutationsRoute(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	familyName := vars["familyName"]

	if r.Method == "GET" {
		writerName := r.URL.Query().Get("writer")
		if writerName == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
	if r.Method == "POST" {
		hdrWriter := r.Header.Get("ctlstore-writer")
		hdrSecret := r.Header.Get("ctlstore-secret")

		payload := struct {
			Cookie      []byte `json:"cookie"`
			CheckCookie []byte `json:"check_cookie"`
			Requests    []struct {
				TableName string                 `json:"table"`
				Delete    bool                   `json:"delete"`
				Values    map[string]interface{} `json:"values"`
			} `json:"mutations"`
		}{}

		rawBody, err := io.ReadAll(r.Body)
		if err != nil {
			writeErrorResponse(err, w)
			return
		}

		err = json.Unmarshal(rawBody, &payload)
		if err != nil {
			writeErrorResponse(&errs.BadRequestError{Err: "JSON Error: " + err.Error()}, w)
			return
		}

		// Could totally put JSON tags into ExecutiveMutationRequest,
		// but I'm paranoid because this is web input. It also sorta
		// breaks the "interface" layer decoupling used to separate the
		// web handler code from the Executive internals.
		totalValues := 0
		unpackedReqs := []ExecutiveMutationRequest{}
		for _, req := range payload.Requests {
			unpackedReqs = append(unpackedReqs, ExecutiveMutationRequest{
				TableName: req.TableName,
				Delete:    req.Delete,
				Values:    req.Values,
			})
			totalValues += len(req.Values)
		}

		stats.Add("mutation-values-received", totalValues, stats.T("writer", hdrWriter))

		err = ee.Exec.Mutate(
			hdrWriter,
			hdrSecret,
			familyName,
			payload.Cookie,
			payload.CheckCookie,
			unpackedReqs)

		_ = err

		if err != nil {
			writeErrorResponse(err, w)
			return
		}
	}
}

func (ee *ExecutiveEndpoint) handleSleepRoute(w http.ResponseWriter, r *http.Request) {
	body := "slept"

	select {
	case <-time.After(60 * time.Second):
		break
	case <-r.Context().Done():
		body = "cancelled"
		break
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(body))
	return
}

func (ee *ExecutiveEndpoint) handleStatusRoute(w http.ResponseWriter, r *http.Request) {
	err := ee.HealthChecker.HealthCheck()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.EventLog("Health check failure: %{error}+v", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	return
}

func (ee *ExecutiveEndpoint) Handler() http.Handler {
	r := mux.NewRouter()

	// instrument all API methods on the executive
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			now := time.Now()
			statusWriter := &statusWriter{writer: w, code: http.StatusOK}
			defer func() {
				delta := time.Now().Sub(now)
				stats.Observe("api-latency", delta,
					stats.T("uri", r.RequestURI),
					stats.T("method", r.Method),
					stats.T("code", strconv.Itoa(statusWriter.code)))
			}()
			next.ServeHTTP(statusWriter, r)
		})
	})

	r.HandleFunc("/cookie", ee.handleCookieRoute).Methods("GET", "POST")
	r.HandleFunc("/families/{familyName}", ee.handleFamilyRoute).Methods("POST")
	r.HandleFunc("/families/{familyName}/tables/{tableName}", ee.handleTableRoute).Methods("POST", "PUT")
	r.HandleFunc("/families/{familyName}/mutations", ee.handleMutationsRoute).Methods("POST")
	r.HandleFunc("/tables", ee.handleTablesRoute).Methods("POST")
	r.HandleFunc("/sleep", ee.handleSleepRoute).Methods("GET")
	r.HandleFunc("/status", ee.handleStatusRoute).Methods("GET")
	r.HandleFunc("/writers/{writerName}", ee.handleWritersRoute).Methods("POST")

	r.HandleFunc("/schema/table/{familyName}/{tableName}", ee.handleTableSchemaRoute).Methods(http.MethodGet)
	r.HandleFunc("/schema/family/{familyName}", ee.handleFamilySchemasRoute).Methods(http.MethodGet)

	r.HandleFunc("/limits/tables", ee.handleTableLimitsRead).Methods("GET")
	r.HandleFunc("/limits/tables/{familyName}/{tableName}", ee.handleTableLimitsUpdate).Methods("POST")
	r.HandleFunc("/limits/tables/{familyName}/{tableName}", ee.handleTableLimitsDelete).Methods("DELETE")

	r.HandleFunc("/limits/writers", ee.handleWriterLimitsRead).Methods("GET")
	r.HandleFunc("/limits/writers/{writerName}", ee.handleWriterLimitsUpdate).Methods("POST")
	r.HandleFunc("/limits/writers/{writerName}", ee.handleWriterLimitsDelete).Methods("DELETE")

	// destructive routes below

	r.HandleFunc("/clear-rows/families/{familyName}", ee.handleClearFamilyRows).Methods("DELETE")
	r.HandleFunc("/clear-rows/families/{familyName}/tables/{tableName}", ee.handleClearTableRows).Methods("DELETE")
	r.HandleFunc("/families/{familyName}/tables/{tableName}", ee.handleDropTable).Methods("DELETE")

	// Limit request body sizes
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > limits.LimitRequestBodySize {
				http.Error(w, "Request too large", http.StatusExpectationFailed)
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, limits.LimitRequestBodySize)
			next.ServeHTTP(w, r)
		})
	})

	return r
}

func (ee *ExecutiveEndpoint) handleTableLimitsRead(w http.ResponseWriter, r *http.Request) {
	handlingErrorDo(w, func() error {
		limits, err := ee.Exec.ReadTableSizeLimits()
		if err != nil {
			return err
		}
		b, err := json.Marshal(limits)
		if err != nil {
			return err
		}
		_, err = w.Write(b)
		return err
	})
}

func (ee *ExecutiveEndpoint) handleTableLimitsUpdate(w http.ResponseWriter, r *http.Request) {
	handlingErrorDo(w, func() error {
		vars := mux.Vars(r)
		familyName := vars["familyName"]
		tableName := vars["tableName"]
		familyName, tableName, err := sanitizeFamilyAndTableNames(familyName, tableName)
		if err != nil {
			return &errs.BadRequestError{Err: err.Error()}
		}
		tsl := limits.TableSizeLimit{Family: familyName, Table: tableName}
		err = json.NewDecoder(r.Body).Decode(&tsl.SizeLimits)
		if err != nil {
			return err
		}
		return ee.Exec.UpdateTableSizeLimit(tsl)
	})
}

func (ee *ExecutiveEndpoint) handleTableLimitsDelete(w http.ResponseWriter, r *http.Request) {
	handlingErrorDo(w, func() error {
		vars := mux.Vars(r)
		familyName := vars["familyName"]
		tableName := vars["tableName"]
		familyName, tableName, err := sanitizeFamilyAndTableNames(familyName, tableName)
		if err != nil {
			return &errs.BadRequestError{Err: err.Error()}
		}
		ft := schema.FamilyTable{Family: familyName, Table: tableName}
		return ee.Exec.DeleteTableSizeLimit(ft)
	})
}

func (ee *ExecutiveEndpoint) handleWriterLimitsRead(w http.ResponseWriter, r *http.Request) {
	handlingErrorDo(w, func() error {
		limits, err := ee.Exec.ReadWriterRateLimits()
		if err != nil {
			return err
		}
		b, err := json.Marshal(limits)
		if err != nil {
			return err
		}
		_, err = w.Write(b)
		return err
	})
}

func (ee *ExecutiveEndpoint) handleWriterLimitsUpdate(w http.ResponseWriter, r *http.Request) {
	handlingErrorDo(w, func() error {
		vars := mux.Vars(r)
		writerName, err := schema.NewWriterName(vars["writerName"])
		if err != nil {
			return &errs.BadRequestError{Err: err.Error()}
		}
		var limit limits.RateLimit
		if err := json.NewDecoder(r.Body).Decode(&limit); err != nil {
			return err
		}
		writerLimit := limits.WriterRateLimit{Writer: writerName.Name, RateLimit: limit}
		return ee.Exec.UpdateWriterRateLimit(writerLimit)
	})
}

func (ee *ExecutiveEndpoint) handleWriterLimitsDelete(w http.ResponseWriter, r *http.Request) {
	handlingErrorDo(w, func() error {
		vars := mux.Vars(r)
		writerName, err := schema.NewWriterName(vars["writerName"])
		if err != nil {
			return &errs.BadRequestError{Err: err.Error()}
		}
		return ee.Exec.DeleteWriterRateLimit(writerName.Name)
	})
}

func handlingErrorDo(w http.ResponseWriter, fn func() error) {
	if err := fn(); err != nil {
		writeErrorResponse(err, w)
	}
}

func (ee *ExecutiveEndpoint) handleDropTable(w http.ResponseWriter, r *http.Request) {
	if !ee.EnableDestructiveSchemaChanges {
		writeErrorResponse(&errs.BadRequestError{Err: "Dropping tables is not enabled."}, w)
		return
	}

	vars := mux.Vars(r)
	// if these panic, Mux is broken and nothing is sacred anymore
	familyName := vars["familyName"]
	tableName := vars["tableName"]
	familyName, tableName, err := sanitizeFamilyAndTableNames(familyName, tableName)
	if err != nil {
		writeErrorResponse(&errs.BadRequestError{Err: err.Error()}, w)
		return
	}

	ft := schema.FamilyTable{Family: familyName, Table: tableName}
	err = ee.Exec.DropTable(ft)
	if err != nil {
		writeErrorResponse(err, w)
		return
	}

	return
}

func (ee *ExecutiveEndpoint) handleClearTableRows(w http.ResponseWriter, r *http.Request) {
	if !ee.EnableDestructiveSchemaChanges {
		writeErrorResponse(&errs.BadRequestError{Err: "Clearing tables is not enabled."}, w)
		return
	}

	vars := mux.Vars(r)
	// if these panic, Mux is broken and nothing is sacred anymore
	familyName := vars["familyName"]
	tableName := vars["tableName"]
	familyName, tableName, err := sanitizeFamilyAndTableNames(familyName, tableName)
	if err != nil {
		writeErrorResponse(&errs.BadRequestError{Err: err.Error()}, w)
		return
	}

	ft := schema.FamilyTable{Family: familyName, Table: tableName}
	err = ee.Exec.ClearTable(ft)
	if err != nil {
		writeErrorResponse(err, w)
		return
	}

	return
}

func (ee *ExecutiveEndpoint) handleClearFamilyRows(w http.ResponseWriter, r *http.Request) {
	if !ee.EnableDestructiveSchemaChanges {
		writeErrorResponse(&errs.BadRequestError{Err: "Clearing tables is not enabled."}, w)
		return
	}

	vars := mux.Vars(r)
	// if these panic, Mux is broken and nothing is sacred anymore
	familyName := vars["familyName"]
	family, err := schema.NewFamilyName(familyName)
	if err != nil {
		writeErrorResponse(&errs.BadRequestError{Err: err.Error()}, w)
		return
	}

	tables, err := ee.Exec.ReadFamilyTableNames(family)
	if err != nil {
		writeErrorResponse(err, w)
		return
	}

	for _, table := range tables {
		err = ee.Exec.ClearTable(table)
		if err != nil {
			writeErrorResponse(err, w)
			return
		}
	}

	return
}

// TODO: does ifCookie override the stored cookie? could cause problems!

func (ee *ExecutiveEndpoint) Close() error {
	return nil
}

func writeErrorResponse(e error, w http.ResponseWriter) {
	status := http.StatusInternalServerError
	resBody := e.Error()

	cause := errors.Cause(e)
	// first check for generic error values
	switch cause {
	case ErrWriterAlreadyExists:
		status = http.StatusConflict
	default:
		// if no generic error values matched, check the error types as well
		switch cause.(type) {
		case *errs.ConflictError:
			status = http.StatusConflict
		case *errs.BadRequestError:
			status = http.StatusBadRequest
		case *errs.NotFoundError:
			status = http.StatusNotFound
		case *errs.RateLimitExceededErr:
			status = http.StatusTooManyRequests
		case *errs.InsufficientStorageErr:
			status = http.StatusInsufficientStorage
		default:
			status = http.StatusInternalServerError
		}

	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(resBody))

	log.EventLog("Error Status %{status}v, Reason: %{reason}v, Internal Error: %{error}+v",
		status, resBody, e.Error())

	return
}
