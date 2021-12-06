package schema

import (
	"fmt"
)

// use newWriterName to construct a writerName
type WriterName struct {
	Name string
}

const (
	MinWriterNameLength = 3
	MaxWriterNameLength = 50
)

var (
	ErrWriterNameTooLong  = fmt.Errorf("Writer names can only be up to %d characters", MaxWriterNameLength)
	ErrWriterNameTooShort = fmt.Errorf("Writer names must be at least %d characters", MinWriterNameLength)
)

func validateWriterName(writerName string) (string, error) {
	if len(writerName) > MaxWriterNameLength {
		return "", ErrWriterNameTooLong
	}
	if len(writerName) < MinWriterNameLength {
		return "", ErrWriterNameTooShort
	}
	return writerName, nil
}

func NewWriterName(name string) (WriterName, error) {
	validated, err := validateWriterName(name)
	if err != nil {
		return WriterName{}, err
	}
	return WriterName{validated}, nil
}

func (wn WriterName) String() string {
	return wn.Name
}
