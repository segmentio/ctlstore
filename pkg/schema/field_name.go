package schema

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	MinFieldNameLength = 1
	MaxFieldNameLength = 30
)

var fieldNameChars = regexp.MustCompile("^$|^[a-z][a-z0-9_]*$")

var (
	ErrFieldNameInvalid  = errors.New("Field names must be only letters, numbers, and underscore")
	ErrFieldNameTooLong  = fmt.Errorf("Field names can only be up to %d characters", MaxFieldNameLength)
	ErrFieldNameTooShort = fmt.Errorf("Field names must be at least %d characters", MinFieldNameLength)
)

type FieldName struct {
	Name string
}

func (f FieldName) String() string {
	return f.Name
}

func NewFieldName(name string) (FieldName, error) {
	normalized, err := normalizeFieldName(name)
	if err != nil {
		return FieldName{}, err
	}
	return FieldName{Name: normalized}, nil
}

func StringifyFieldNames(fns []FieldName) []string {
	out := make([]string, len(fns))
	for i, fn := range fns {
		out[i] = fn.Name
	}
	return out
}

func normalizeFieldName(fieldName string) (string, error) {
	lowered := strings.ToLower(fieldName)
	if !fieldNameChars.MatchString(lowered) {
		return "", ErrFieldNameInvalid
	}
	if len(lowered) > MaxFieldNameLength {
		return "", ErrFieldNameTooLong
	}
	if len(lowered) < MinFieldNameLength {
		return "", ErrFieldNameTooShort
	}
	return lowered, nil
}
