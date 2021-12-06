package errs

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusCodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code int
	}{
		{
			name: "generic",
			err:  errors.New("foo"),
			code: http.StatusInternalServerError,
		},
		{
			name: "conflict",
			err:  ConflictError{},
			code: http.StatusConflict,
		},
		{
			name: "bad request",
			err:  BadRequestError{},
			code: http.StatusBadRequest,
		},
		{
			name: "not found",
			err:  NotFound("foo"),
			code: http.StatusNotFound,
		},
		{
			name: "rate limited",
			err:  RateLimitExceededErr{},
			code: http.StatusTooManyRequests,
		},
		{
			name: "insufficient storage",
			err:  InsufficientStorageErr{},
			code: http.StatusInsufficientStorage,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := fmt.Errorf("wrapped: %w", test.err)
			code := statusCode(err)
			require.Equal(t, test.code, code)
		})
	}
}
