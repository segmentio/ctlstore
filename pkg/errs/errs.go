package errs

import (
	"context"
	"fmt"

	"github.com/segmentio/errors-go"
	"github.com/segmentio/stats"
)

const (
	defaultErrName = "errors"
)

const (
	// these error types are handy when using errors-go
	ErrTypeTemporary = "Temporary"
	ErrTypePermanent = "Permanent"
)

func IsCanceled(err error) bool {
	return err != nil && errors.Cause(err) == context.Canceled
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

// These are here because there's a need for a set of errors that have roughly
// REST/HTTP compatibility, but aren't directly coupled to that interface. Lower
// layers of the system can generate these errors while still making sense in
// any context.
type baseError struct {
	Err string
}

type ConflictError baseError

func (e ConflictError) Error() string {
	return e.Err
}

type BadRequestError baseError

func (e BadRequestError) Error() string {
	return e.Err
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

type InsufficientStorageErr baseError

func (e InsufficientStorageErr) Error() string {
	return e.Err
}
