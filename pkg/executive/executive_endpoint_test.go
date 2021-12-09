package executive_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/executive"
	"github.com/segmentio/ctlstore/pkg/executive/fakes"
	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/stretchr/testify/require"
)

type testExecEndpointHandlerAtom struct {
	Desc               string
	Path               string
	Method             string
	Vars               map[string]string
	ExpectedStatusCode int
	JSONBody           interface{}
	RawBody            []byte
	PreFunc            func(t *testing.T, atom *testExecEndpointHandlerAtom)
	PostFunc           func(t *testing.T, atom *testExecEndpointHandlerAtom)

	rr *httptest.ResponseRecorder
	ei *fakes.FakeExecutiveInterface
	ee *executive.ExecutiveEndpoint
}

// Use _t here because it's easy to accidentally do t in the closures
func TestExecEndpointHandler(_t *testing.T) {
	///////////////////////////////////////////
	// Define the table
	atoms := []testExecEndpointHandlerAtom{
		{
			Desc:               "Read Writer Limits Success",
			Path:               "/limits/writers",
			Method:             http.MethodGet,
			ExpectedStatusCode: http.StatusOK,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.ReadWriterRateLimitsReturns(limits.WriterRateLimits{
					Global: limits.RateLimit{
						Amount: 1000,
						Period: time.Minute,
					},
				}, nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.ReadWriterRateLimitsCallCount())
				var wrl limits.WriterRateLimits
				require.NoError(t, json.NewDecoder(atom.rr.Body).Decode(&wrl))
				require.EqualValues(t, limits.WriterRateLimits{
					Global: limits.RateLimit{
						Amount: 1000,
						Period: time.Minute,
					},
				}, wrl)
			},
		},
		{
			Desc:               "Read Writer Limits Failure",
			Path:               "/limits/writers",
			Method:             http.MethodGet,
			ExpectedStatusCode: http.StatusInternalServerError,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.ReadWriterRateLimitsReturns(limits.WriterRateLimits{}, errors.New("failure"))
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.ReadWriterRateLimitsCallCount())
				require.EqualValues(t, "failure", atom.rr.Body.String())
			},
		},
		{
			Desc:   "Update Writer Limits Success",
			Path:   "/limits/writers/mywriter",
			Method: http.MethodPost,
			JSONBody: map[string]interface{}{
				"amount": 1000,
				"period": "1m",
			},
			ExpectedStatusCode: http.StatusOK,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.UpdateWriterRateLimitReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.UpdateWriterRateLimitCallCount())
				wrl := atom.ei.UpdateWriterRateLimitArgsForCall(0)
				require.EqualValues(t, limits.WriterRateLimit{
					Writer: "mywriter",
					RateLimit: limits.RateLimit{
						Amount: 1000,
						Period: time.Minute,
					},
				}, wrl)
			},
		},
		{
			Desc:   "Update Writer Limits Failure",
			Path:   "/limits/writers/mywriter",
			Method: http.MethodPost,
			JSONBody: map[string]interface{}{
				"amount": 1000,
				"period": time.Minute,
			},
			ExpectedStatusCode: http.StatusInternalServerError,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.UpdateWriterRateLimitReturns(errors.New("failure"))
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.UpdateWriterRateLimitCallCount())
				wrl := atom.ei.UpdateWriterRateLimitArgsForCall(0)
				require.EqualValues(t, limits.WriterRateLimit{
					Writer: "mywriter",
					RateLimit: limits.RateLimit{
						Amount: 1000,
						Period: time.Minute,
					},
				}, wrl)
				require.Equal(t, "failure", atom.rr.Body.String())
			},
		},
		{
			Desc:   "Update Writer Limits Checks Writer Name",
			Path:   "/limits/writers/no",
			Method: http.MethodPost,
			JSONBody: map[string]interface{}{
				"amount": 1000,
				"period": time.Minute,
			},
			ExpectedStatusCode: http.StatusBadRequest,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.UpdateWriterRateLimitReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 0, atom.ei.UpdateWriterRateLimitCallCount())
				require.Equal(t, "Writer names must be at least 3 characters", atom.rr.Body.String())
			},
		},
		{
			Desc:   "Update Writer Limits Invalid Payload",
			Path:   "/limits/writers/mywriter",
			Method: http.MethodPost,
			JSONBody: map[string]interface{}{
				"amount": 1000,
				"period": "a year",
			},
			ExpectedStatusCode: http.StatusInternalServerError,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.UpdateWriterRateLimitReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 0, atom.ei.UpdateWriterRateLimitCallCount())
				require.Equal(t, "invalid period: 'a year'", atom.rr.Body.String())
			},
		},
		{
			Desc:               "Delete Writer Limit Success",
			Path:               "/limits/writers/mywriter",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusOK,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.DeleteWriterRateLimitReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.DeleteWriterRateLimitCallCount())
				writerName := atom.ei.DeleteWriterRateLimitArgsForCall(0)
				require.EqualValues(t, "mywriter", writerName)
			},
		},
		{
			Desc:               "Delete Writer Limit Failure",
			Path:               "/limits/writers/mywriter",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusInternalServerError,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.DeleteWriterRateLimitReturns(errors.New("failure"))
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.DeleteWriterRateLimitCallCount())
				writerName := atom.ei.DeleteWriterRateLimitArgsForCall(0)
				require.EqualValues(t, "mywriter", writerName)
				require.EqualValues(t, "failure", atom.rr.Body.String())
			},
		},
		{
			Desc:               "Delete Writer Limit Checks Writer",
			Path:               "/limits/writers/no",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusBadRequest,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 0, atom.ei.DeleteWriterRateLimitCallCount())
				require.EqualValues(t, "Writer names must be at least 3 characters", atom.rr.Body.String())
			},
		},
		{
			Desc:               "Read Table Limits Success",
			Path:               "/limits/tables",
			Method:             http.MethodGet,
			ExpectedStatusCode: http.StatusOK,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.ReadTableSizeLimitsReturns(limits.TableSizeLimits{
					Global: limits.SizeLimits{
						MaxSize:  1000,
						WarnSize: 50,
					},
				}, nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.ReadTableSizeLimitsCallCount())
				var tsl limits.TableSizeLimits
				require.NoError(t, json.NewDecoder(atom.rr.Body).Decode(&tsl))
				require.EqualValues(t, limits.TableSizeLimits{
					Global: limits.SizeLimits{
						MaxSize:  1000,
						WarnSize: 50,
					},
				}, tsl)
			},
		},
		{
			Desc:               "Read Table Limits Failure",
			Path:               "/limits/tables",
			Method:             http.MethodGet,
			ExpectedStatusCode: http.StatusInternalServerError,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.ReadTableSizeLimitsReturns(limits.TableSizeLimits{}, errors.New("failure"))
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.ReadTableSizeLimitsCallCount())
			},
		},
		{
			Desc:   "Update Table Limit Success",
			Path:   "/limits/tables/myfamily/mytable",
			Method: http.MethodPost,
			JSONBody: map[string]interface{}{
				"max-size":  1000,
				"warn-size": 50,
			},
			ExpectedStatusCode: http.StatusOK,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.UpdateTableSizeLimitReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.UpdateTableSizeLimitCallCount())
				tsl := atom.ei.UpdateTableSizeLimitArgsForCall(0)
				require.EqualValues(t, limits.TableSizeLimit{
					Family: "myfamily",
					Table:  "mytable",
					SizeLimits: limits.SizeLimits{
						MaxSize:  1000,
						WarnSize: 50,
					},
				}, tsl)
			},
		},
		{
			Desc:   "Update Table Limit Failure",
			Path:   "/limits/tables/myfamily/mytable",
			Method: http.MethodPost,
			JSONBody: map[string]interface{}{
				"max-size":  1000,
				"warn-size": 50,
			},
			ExpectedStatusCode: http.StatusInternalServerError,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.UpdateTableSizeLimitReturns(errors.New("failure"))
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.UpdateTableSizeLimitCallCount())
				tsl := atom.ei.UpdateTableSizeLimitArgsForCall(0)
				require.EqualValues(t, limits.TableSizeLimit{
					Family: "myfamily",
					Table:  "mytable",
					SizeLimits: limits.SizeLimits{
						MaxSize:  1000,
						WarnSize: 50,
					},
				}, tsl)
				require.EqualValues(t, "failure", atom.rr.Body.String())
			},
		},
		{
			Desc:               "Update Table Limit Checks Family",
			Path:               "/limits/tables/my___family/mytable",
			Method:             http.MethodPost,
			ExpectedStatusCode: http.StatusBadRequest,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 0, atom.ei.UpdateTableSizeLimitCallCount())
				require.EqualValues(t,
					"sanitize family: Family names must be only letters, numbers, and single underscore",
					atom.rr.Body.String())
			},
		},
		{
			Desc:               "Update Table Limit Checks Table",
			Path:               "/limits/tables/myfamily/my___table",
			Method:             http.MethodPost,
			ExpectedStatusCode: http.StatusBadRequest,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 0, atom.ei.UpdateTableSizeLimitCallCount())
				require.EqualValues(t,
					"sanitize table: Table names must be only letters, numbers, and single underscore",
					atom.rr.Body.String())
			},
		},
		{
			Desc:               "Delete Table Limit Success",
			Path:               "/limits/tables/myfamily/mytable",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusOK,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.DeleteTableSizeLimitReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.DeleteTableSizeLimitCallCount())
				ft := atom.ei.DeleteTableSizeLimitArgsForCall(0)
				require.EqualValues(t, schema.FamilyTable{
					Family: "myfamily",
					Table:  "mytable",
				}, ft)
			},
		},
		{
			Desc:               "Delete Table Limit Failure",
			Path:               "/limits/tables/myfamily/mytable",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusInternalServerError,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.DeleteTableSizeLimitReturns(errors.New("failure"))
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.DeleteTableSizeLimitCallCount())
				ft := atom.ei.DeleteTableSizeLimitArgsForCall(0)
				require.EqualValues(t, schema.FamilyTable{
					Family: "myfamily",
					Table:  "mytable",
				}, ft)
				require.EqualValues(t, "failure", atom.rr.Body.String())
			},
		},
		{
			Desc:               "Delete Table Limit Checks Family",
			Path:               "/limits/tables/my___family/mytable",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusBadRequest,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 0, atom.ei.DeleteTableSizeLimitCallCount())
				require.EqualValues(t,
					"sanitize family: Family names must be only letters, numbers, and single underscore",
					atom.rr.Body.String())
			},
		},
		{
			Desc:               "Delete Table Limit Checks Table",
			Path:               "/limits/tables/myfamily/my___table",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusBadRequest,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 0, atom.ei.DeleteTableSizeLimitCallCount())
				require.EqualValues(t,
					"sanitize table: Table names must be only letters, numbers, and single underscore",
					atom.rr.Body.String())
			},
		},

		{
			Desc:               "Create Family Success",
			Path:               "/families/foo",
			Method:             "POST",
			ExpectedStatusCode: 200,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.CreateFamilyReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				if want, got := 1, atom.ei.CreateFamilyCallCount(); want != got {
					t.Errorf("Expected CreateFamily call count to be %v, was %v", want, got)
				}

				if want, got := 200, atom.rr.Code; want != got {
					t.Errorf("Expected status code to be %v, was %v", want, got)
				}
			},
		},
		{
			Desc:               "Create Family Already Exists",
			Path:               "/families/foo",
			Method:             "POST",
			ExpectedStatusCode: 409,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.CreateFamilyReturns(&errs.ConflictError{Err: "Family already exists"})
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				if want, got := 1, atom.ei.CreateFamilyCallCount(); want != got {
					t.Errorf("Expected CreateFamily call count to be %v, was %v", want, got)
				}
			},
		},
		{
			Desc:               "Create Family Unknown Error",
			Path:               "/families/foo",
			Method:             "POST",
			ExpectedStatusCode: 500,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.CreateFamilyReturns(errors.New("Hello World"))
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				if want, got := 1, atom.ei.CreateFamilyCallCount(); want != got {
					t.Errorf("Expected CreateFamily call count to be %v, was %v", want, got)
				}
			},
		},
		{
			Desc:   "Create Table Success",
			Path:   "/families/foo/tables/bar",
			Method: "POST",
			JSONBody: map[string]interface{}{
				"fields": [][]interface{}{
					// Put these in the wrong order intentionally because order
					// does matter!
					{"field3", "decimal"},
					{"field2", "integer"},
					{"field1", "string"},
				},
				"keyFields": []string{"field2"},
			},
			ExpectedStatusCode: 200,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.CreateTableReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				if want, got := 1, atom.ei.CreateTableCallCount(); want != got {
					// Fatal cuz if not it'll panic below
					t.Fatalf("Expected CreateTable call count to be %v, was %v", want, got)
				}

				// Too trivial to test the first two arguments, but I want to
				// make sure the field transposition is right
				_, _, fieldNames, fieldTypes, keyFields := atom.ei.CreateTableArgsForCall(0)

				if want, got := []string{"field3", "field2", "field1"}, fieldNames; !reflect.DeepEqual(want, got) {
					t.Errorf("Expected CreateTable fieldNames to be %v, was %v", want, got)
				}

				fieldTypesExpect := []schema.FieldType{
					schema.FTDecimal,
					schema.FTInteger,
					schema.FTString,
				}
				if want, got := fieldTypesExpect, fieldTypes; !reflect.DeepEqual(want, got) {
					t.Errorf("Expected CreateTable fieldTypes to be %v, was %v", want, got)
				}

				if want, got := []string{"field2"}, keyFields; !reflect.DeepEqual(want, got) {
					t.Errorf("Expected CreateTable keyFields to be %v, was %v", want, got)
				}
			},
		},
		{
			Desc:   "Alter Table Success",
			Path:   "/families/foo/tables/bar",
			Method: "PUT",
			JSONBody: map[string]interface{}{
				"fields": [][]interface{}{
					{"field4", "decimal"},
					{"field5", "integer"},
				},
			},
			ExpectedStatusCode: 200,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.AddFieldsReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				if want, got := 1, atom.ei.AddFieldsCallCount(); want != got {
					// Fatal cuz if not it'll panic below
					t.Fatalf("Expected AddFields call count to be %v, was %v", want, got)
				}

				a1, a2, a3, a4 := atom.ei.AddFieldsArgsForCall(0)
				if want, got := "foo", a1; want != got {
					t.Errorf("Expected: %v, got %v", want, got)
				}
				if want, got := "bar", a2; want != got {
					t.Errorf("Expected: %v, got %v", want, got)
				}
				if want, got := []string{"field4", "field5"}, a3; !reflect.DeepEqual(want, got) {
					t.Errorf("Expected: %v, got %v", want, got)
				}
				expectedFieldTypes := []schema.FieldType{
					schema.FTDecimal,
					schema.FTInteger,
				}
				if want, got := expectedFieldTypes, a4; !reflect.DeepEqual(want, got) {
					t.Errorf("Expected: %v, got %v", want, got)
				}
			},
		},
		{
			Desc:               "Fetch cookie + writer found",
			Path:               "/cookie",
			Method:             "GET",
			ExpectedStatusCode: 200,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.GetWriterCookieReturns([]byte("hello"), nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				if want, got := 1, atom.ei.GetWriterCookieCallCount(); want != got {
					// Fatal cuz if not it'll panic below
					t.Fatalf("Expected GetWriterCookie call count to be %v, was %v", want, got)
				}

				a1, a2 := atom.ei.GetWriterCookieArgsForCall(0)
				if want, got := "writer1", a1; want != got {
					t.Errorf("Expected: %v, got %v", want, got)
				}

				if want, got := "", a2; want != got {
					t.Errorf("Expected: %v, got %v", want, got)
				}

				if diff := cmp.Diff(atom.rr.Body.Bytes(), []byte("hello")); diff != "" {
					t.Errorf("Body differs\n%s", diff)
				}

			},
		},
		{
			Desc:               "Set cookie + writer not found",
			Path:               "/cookie",
			Method:             "POST",
			ExpectedStatusCode: 404,
			RawBody:            []byte("greetings"),
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.SetWriterCookieReturns(&errs.NotFoundError{})
				atom.ei.GetWriterCookieReturns([]byte("greetings"), nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				if want, got := 1, atom.ei.SetWriterCookieCallCount(); want != got {
					// Fatal cuz if not it'll panic below
					t.Fatalf("Expected SetWriterCookie call count to be %v, was %v", want, got)
				}
			},
		},
		{
			Desc:               "Set cookie + writer found",
			Path:               "/cookie",
			Method:             "POST",
			ExpectedStatusCode: 200,
			RawBody:            []byte("greetings"),
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.SetWriterCookieReturns(nil)
				atom.ei.GetWriterCookieReturns([]byte("greetings"), nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				if want, got := 1, atom.ei.SetWriterCookieCallCount(); want != got {
					// Fatal cuz if not it'll panic below
					t.Fatalf("Expected SetWriterCookie call count to be %v, was %v", want, got)
				}

				a1, a2, a3 := atom.ei.SetWriterCookieArgsForCall(0)
				if want, got := "writer1", a1; want != got {
					t.Errorf("Expected: %v, got %v", want, got)
				}

				if want, got := "", a2; want != got {
					t.Errorf("Expected: %v, got %v", want, got)
				}

				if diff := cmp.Diff(a3, []byte("greetings")); diff != "" {
					t.Errorf("Cookie differs\n%s", diff)
				}

			},
		},
		{
			Desc:               "Register Writer",
			Path:               "/writers/writer1",
			Method:             "POST",
			RawBody:            []byte("secret1"),
			ExpectedStatusCode: 200,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.RegisterWriterReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				a1, a2 := atom.ei.RegisterWriterArgsForCall(0)
				require.Equal(t, "writer1", a1)
				require.Equal(t, "secret1", a2)
			},
		},
		{
			Desc:               "Register Writer Failure",
			Path:               "/writers/writer1",
			Method:             "POST",
			RawBody:            []byte("secret1"),
			ExpectedStatusCode: http.StatusConflict,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.RegisterWriterReturns(executive.ErrWriterAlreadyExists)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				a1, a2 := atom.ei.RegisterWriterArgsForCall(0)
				require.Equal(t, "writer1", a1)
				require.Equal(t, "secret1", a2)
			},
		},
		{
			Desc:   "Mutation Success",
			Path:   "/families/foo/mutations",
			Method: "POST",
			JSONBody: map[string]interface{}{
				"cookie": []byte("cookie1"),
				"mutations": []map[string]interface{}{
					{
						"table":  "table1",
						"delete": false,
						"values": map[string]interface{}{
							"foo-field": "foo-value",
							"bar-field": "bar-value",
						},
					},
					{
						"table":  "table2",
						"delete": true,
						"values": map[string]interface{}{
							"baz-field": "baz-value",
							"bim-field": "bim-value",
						},
					},
				},
			},
			ExpectedStatusCode: 200,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.MutateReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				if want, got := 1, atom.ei.MutateCallCount(); want != got {
					// Fatal cuz if not it'll panic below
					t.Fatalf("Expected Mutate call count to be %v, was %v", want, got)
				}

				a1, a2, a3, a4, a5, a6 := atom.ei.MutateArgsForCall(0)
				if want, got := "writer1", a1; want != got {
					t.Errorf("Expected: %v, got %v", want, got)
				}
				if want, got := "", a2; want != got {
					t.Errorf("Expected: %v, got %v", want, got)
				}
				if want, got := "foo", a3; want != got {
					t.Errorf("Expected: %v, got %v", want, got)
				}
				if want, got := []byte("cookie1"), a4; !reflect.DeepEqual(want, got) {
					t.Errorf("Expected: %v, got %v", want, got)
				}
				if want, got := ([]byte)(nil), a5; !reflect.DeepEqual(want, got) {
					t.Errorf("Expected: %v, got %v", want, got)
				}

				expectedRequests := []executive.ExecutiveMutationRequest{
					{
						TableName: "table1",
						Delete:    false,
						Values: map[string]interface{}{
							"foo-field": "foo-value",
							"bar-field": "bar-value",
						},
					},
					{
						TableName: "table2",
						Delete:    true,
						Values: map[string]interface{}{
							"bim-field": "bim-value",
							"baz-field": "baz-value",
						},
					},
				}
				if want, got := expectedRequests, a6; !reflect.DeepEqual(want, got) {
					t.Errorf("Expected: %v, got %v", want, got)
				}
			},
		},
		{
			Desc:               "Clear Table Success",
			Path:               "/clear-rows/families/myfamily/tables/mytable",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusOK,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.ClearTableReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.ClearTableCallCount())
				ft := atom.ei.ClearTableArgsForCall(0)
				require.EqualValues(t, schema.FamilyTable{
					Family: "myfamily",
					Table:  "mytable",
				}, ft)
			},
		},
		{
			Desc:               "Clear Table Failure",
			Path:               "/clear-rows/families/myfamily/tables/mytable",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusInternalServerError,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.ClearTableReturns(errors.New("failure"))
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.ClearTableCallCount())
				ft := atom.ei.ClearTableArgsForCall(0)
				require.EqualValues(t, schema.FamilyTable{
					Family: "myfamily",
					Table:  "mytable",
				}, ft)
				require.EqualValues(t, "failure", atom.rr.Body.String())
			},
		},
		{
			Desc:               "Clear Table Checks Family",
			Path:               "/clear-rows/families/my___family/tables/mytable",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusBadRequest,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 0, atom.ei.ClearTableCallCount())
				require.EqualValues(t,
					"sanitize family: Family names must be only letters, numbers, and single underscore",
					atom.rr.Body.String())
			},
		},
		{
			Desc:               "Clear Table Checks Table",
			Path:               "/clear-rows/families/myfamily/tables/my___table",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusBadRequest,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 0, atom.ei.ClearTableCallCount())
				require.EqualValues(t,
					"sanitize table: Table names must be only letters, numbers, and single underscore",
					atom.rr.Body.String())
			},
		},
		{
			Desc:               "Clear Table Checks Errors when not enabled",
			Path:               "/clear-rows/families/myfamily/tables/mytable",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusBadRequest,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ee.EnableDestructiveSchemaChanges = false
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 0, atom.ei.ClearTableCallCount())
				require.EqualValues(t,
					"Clearing tables is not enabled.",
					atom.rr.Body.String())
			},
		},
		{
			Desc:               "Clear Family Success",
			Path:               "/clear-rows/families/myfamily",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusOK,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.ReadFamilyTableNamesReturns([]schema.FamilyTable{{
					Family: "myfamily",
					Table:  "mytable",
				}}, nil)
				atom.ei.ClearTableReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.ReadFamilyTableNamesCallCount())
				require.EqualValues(t, 1, atom.ei.ClearTableCallCount())

				fam := atom.ei.ReadFamilyTableNamesArgsForCall(0)
				require.EqualValues(t, schema.FamilyName{
					Name: "myfamily",
				}, fam)

				ft := atom.ei.ClearTableArgsForCall(0)
				require.EqualValues(t, schema.FamilyTable{
					Family: "myfamily",
					Table:  "mytable",
				}, ft)
			},
		},
		{
			Desc:               "Clear Family Failure",
			Path:               "/clear-rows/families/myfamily",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusInternalServerError,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.ReadFamilyTableNamesReturns([]schema.FamilyTable{}, errors.New("failure"))
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.ReadFamilyTableNamesCallCount())
				require.EqualValues(t, 0, atom.ei.ClearTableCallCount())

				fam := atom.ei.ReadFamilyTableNamesArgsForCall(0)
				require.EqualValues(t, schema.FamilyName{
					Name: "myfamily",
				}, fam)
				require.EqualValues(t, "failure", atom.rr.Body.String())
			},
		},
		{
			Desc:               "Clear Family Checks Family",
			Path:               "/clear-rows/families/my___family",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusBadRequest,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 0, atom.ei.ReadFamilyTableNamesCallCount())
				require.EqualValues(t, 0, atom.ei.ClearTableCallCount())
				require.EqualValues(t,
					"Family names must be only letters, numbers, and single underscore",
					atom.rr.Body.String())
			},
		},
		{
			Desc:               "Clear Family Checks Errors when not enabled",
			Path:               "/clear-rows/families/myfamily",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusBadRequest,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ee.EnableDestructiveSchemaChanges = false
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 0, atom.ei.ReadFamilyTableNamesCallCount())
				require.EqualValues(t, 0, atom.ei.ClearTableCallCount())
				require.EqualValues(t,
					"Clearing tables is not enabled.",
					atom.rr.Body.String())
			},
		},
		{
			Desc:               "Drop Table Success",
			Path:               "/families/myfamily/tables/mytable",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusOK,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.DropTableReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.DropTableCallCount())
				ft := atom.ei.DropTableArgsForCall(0)
				require.EqualValues(t, schema.FamilyTable{
					Family: "myfamily",
					Table:  "mytable",
				}, ft)
			},
		},
		{
			Desc:               "Drop Table Failure",
			Path:               "/families/myfamily/tables/mytable",
			Method:             http.MethodDelete,
			ExpectedStatusCode: http.StatusInternalServerError,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.DropTableReturns(errors.New("boom"))
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.DropTableCallCount())
				ft := atom.ei.DropTableArgsForCall(0)
				require.EqualValues(t, schema.FamilyTable{
					Family: "myfamily",
					Table:  "mytable",
				}, ft)
			},
		},
		{
			Desc:               "Get Table Schema Success",
			Path:               "/schema/table/foofamily/bartable",
			Method:             http.MethodGet,
			ExpectedStatusCode: http.StatusOK,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				ret := &schema.Table{
					Family: "foofamily",
					Name:   "bartable",
					Fields: [][]string{
						{"field1", "string"},
					},
					KeyFields: []string{"field1"},
				}
				atom.ei.TableSchemaReturns(ret, nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.TableSchemaCallCount())
				ret := &schema.Table{
					Family: "foofamily",
					Name:   "bartable",
					Fields: [][]string{
						{"field1", "string"},
					},
					KeyFields: []string{"field1"},
				}
				bs, err := json.Marshal(ret)
				require.NoError(t, err)
				require.True(t, bytes.Equal(bs, atom.rr.Body.Bytes()))
			},
		},
		{
			Desc:               "Get Table Schema Error",
			Path:               "/schema/table/foofamily/bartable",
			Method:             http.MethodGet,
			ExpectedStatusCode: http.StatusNotFound,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.TableSchemaReturns(nil, fmt.Errorf("boom: %w", executive.ErrTableDoesNotExist))
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.TableSchemaCallCount())
				require.Equal(t, "boom: table does not exist\n", atom.rr.Body.String())
			},
		},
		{
			Desc:               "Get Family Schema Success",
			Path:               "/schema/family/foofamily",
			Method:             http.MethodGet,
			ExpectedStatusCode: http.StatusOK,
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				ret := []schema.Table{
					{
						Family: "foofamily",
						Name:   "bartable",
						Fields: [][]string{
							{"field1", "string"},
						},
						KeyFields: []string{"field1"},
					},
					{
						Family: "foofamily",
						Name:   "bartable2",
						Fields: [][]string{
							{"field1", "string"},
							{"field2", "integer"},
						},
						KeyFields: []string{"field1"},
					},
				}
				atom.ei.FamilySchemasReturns(ret, nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 0, atom.ei.TableSchemaCallCount())
				require.EqualValues(t, 1, atom.ei.FamilySchemasCallCount())
				ret := []schema.Table{
					{
						Family: "foofamily",
						Name:   "bartable",
						Fields: [][]string{
							{"field1", "string"},
						},
						KeyFields: []string{"field1"},
					},
					{
						Family: "foofamily",
						Name:   "bartable2",
						Fields: [][]string{
							{"field1", "string"},
							{"field2", "integer"},
						},
						KeyFields: []string{"field1"},
					},
				}
				bs, err := json.Marshal(ret)
				require.NoError(t, err)
				require.EqualValues(t, string(bs), atom.rr.Body.String())
			},
		},
		{
			Desc:               "Create Tables Success",
			Path:               "/tables",
			Method:             http.MethodPost,
			ExpectedStatusCode: http.StatusOK,
			JSONBody: []map[string]interface{}{
				{
					"family": "foofamily",
					"name":   "bartable",
					"fields": [][]interface{}{
						{"field1", "string"},
					},
					"keyFields": []string{"field1"},
				},
				{
					"family": "foofamily",
					"name":   "bartable2",
					"fields": [][]interface{}{
						{"field2", "integer"},
						{"field1", "string"},
					},
					"keyFields": []string{"field1"},
				},
			},
			PreFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				atom.ei.CreateTablesReturns(nil)
			},
			PostFunc: func(t *testing.T, atom *testExecEndpointHandlerAtom) {
				require.EqualValues(t, 1, atom.ei.CreateTablesCallCount())

				tables := atom.ei.CreateTablesArgsForCall(0)
				for i, table := range tables {
					var expectedTableName string
					var expectedFields [][]string
					var expectedKeyFields []string
					switch i {
					case 0:
						expectedTableName = "bartable"
						expectedFields = [][]string{{"field1", "string"}}
						expectedKeyFields = []string{"field1"}
					case 1:
						expectedTableName = "bartable2"
						expectedFields = [][]string{{"field2", "integer"}, {"field1", "string"}}
						expectedKeyFields = []string{"field1"}
					}
					require.EqualValues(t, "foofamily", table.Family)
					require.EqualValues(t, expectedTableName, table.Name)
					if want, got := expectedFields, table.Fields; !reflect.DeepEqual(want, got) {
						t.Errorf("Expected CreateTableSchemas Fields to be %v, was %v", want, got)
					}
					if want, got := expectedKeyFields, table.KeyFields; !reflect.DeepEqual(want, got) {
						t.Errorf("Expected CreateTableSchemas KeyFields to be %v, was %v", want, got)
					}
				}
			},
		},
	}

	///////////////////////////////////////////////////
	// Execute the table
	for _, atom := range atoms {
		a := &atom
		_t.Run(a.Desc, func(t *testing.T) {
			// Why does this bug in Parallel mode? Is it because of
			// closure variable capture?
			// t.Parallel()

			var body []byte
			var err error
			var contentType string

			if a.JSONBody != nil {
				body, err = json.Marshal(a.JSONBody)
				if err != nil {
					t.Fatalf("Couldn't marshal JSON body: %v", err)
				}
				contentType = "application/json"
			} else {
				body = a.RawBody
			}

			req, err := http.NewRequest(a.Method, a.Path, bytes.NewReader(body))
			if err != nil {
				t.Fatalf("Unexpected error creating request: %v", err)
			}

			req.Header.Set("ctlstore-writer", "writer1")
			req.Header.Set("ctlstore-secret", "")

			if contentType != "" {
				req.Header.Set("content-type", contentType)
			}

			a.ei = new(fakes.FakeExecutiveInterface)
			a.ee = &executive.ExecutiveEndpoint{Exec: a.ei, EnableDestructiveSchemaChanges: true}
			a.rr = httptest.NewRecorder()

			if a.PreFunc != nil {
				a.PreFunc(t, a)
			}

			a.ee.Handler().ServeHTTP(a.rr, req)

			require.EqualValues(t, a.ExpectedStatusCode, a.rr.Code)

			if a.PostFunc != nil {
				a.PostFunc(t, a)
			}
		})
	}
}
