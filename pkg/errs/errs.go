package errs

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/segmentio/stats/v4"
)

const (
	defaultErrName = "errors"
)

type ErrTypeTemporary struct{ Err error }

func (e ErrTypeTemporary) Error() string {
	return e.Err.Error()
}

func (e ErrTypeTemporary) Is(target error) bool {
	_, ok := target.(ErrTypeTemporary)
	return ok
}

type ErrTypePermanent struct{ Err error }

func (e ErrTypePermanent) Error() string {
	return e.Err.Error()
}

func (e ErrTypePermanent) Is(target error) bool {
	_, ok := target.(ErrTypePermanent)
	return ok
}

func IsCanceled(err error) bool {
	return errors.Is(err, context.Canceled)
}

// IncrDefault increments the default error metric
func IncrDefault(tags ...stats.Tag) {
	Incr(defaultErrName, tags...)
}

// Incr increments an error metric, along with the default error metric
func Incr(name string, tags ...stats.Tag) {
	stats.Incr(name, tags...)
	if name == defaultErrName {
		// don't increment the default error twice
		return
	}
	// add a tag to indicate the name of the original error. We can then
	// view that tag in datadog to figure out what the error was.
	newTags := make([]stats.Tag, len(tags), len(tags)+1)
	copy(newTags, tags)
	newTags = append(newTags, stats.T("error", name))
	stats.Incr(defaultErrName, newTags...)
}

func statusCode(err error) int {
	var coder StatusCoder
	if errors.As(err, &coder) {
		return coder.StatusCode()
	}
	return http.StatusInternalServerError
}

// These are here because there's a need for a set of errors that have roughly
// REST/HTTP compatibility, but aren't directly coupled to that interface. Lower
// layers of the system can generate these errors while still making sense in
// any context.

type StatusCoder interface {
	StatusCode() int
}

type baseError struct {
	StatusCoder
	Err string
}

func (b baseError) StatusCode() int {
	return http.StatusInternalServerError
}

type ConflictError baseError

func (e ConflictError) Error() string {
	return e.Err
}

func (e ConflictError) StatusCode() int {
	return http.StatusConflict
}

type BadRequestError baseError

func (e BadRequestError) Error() string {
	return e.Err
}

func (e BadRequestError) StatusCode() int {
	return http.StatusBadRequest
}

func BadRequest(format string, args ...interface{}) error {
	return &BadRequestError{
		Err: fmt.Sprintf(format, args...),
	}
}

type NotFoundError baseError

func (e NotFoundError) Error() string {
	return e.Err
}

func (e NotFoundError) StatusCode() int {
	return http.StatusNotFound
}

func NotFound(format string, args ...interface{}) error {
	return &NotFoundError{
		Err: fmt.Sprintf(format, args...),
	}
}

type PayloadTooLargeError baseError

func (e PayloadTooLargeError) Error() string {
	return e.Err
}

type RateLimitExceededErr baseError

func (e RateLimitExceededErr) Error() string {
	return e.Err
}

func (e RateLimitExceededErr) StatusCode() int {
	return http.StatusTooManyRequests
}

type InsufficientStorageErr baseError

func (e InsufficientStorageErr) Error() string {
	return e.Err
}

func (e InsufficientStorageErr) StatusCode() int {
	return http.StatusInsufficientStorage
}
