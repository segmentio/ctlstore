package schema

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	MinTableNameLength = 3
	MaxTableNameLength = 50
)

// use newTableName to construct a tableName
type TableName struct {
	Name string
}

var TableNameZero = TableName{}

var tableNameChars = regexp.MustCompile("^$|^[a-z][a-z0-9_]*$")

var (
	ErrTableNameInvalid  = errors.New("Table names must be only letters, numbers, and single underscore")
	ErrTableNameTooLong  = fmt.Errorf("Table names can only be up to %d characters", MaxTableNameLength)
	ErrTableNameTooShort = fmt.Errorf("Table names must be at least %d characters", MinTableNameLength)
)

func NewTableName(name string) (TableName, error) {
	normalized, err := normalizeTableName(name)
	if err != nil {
		return TableNameZero, err
	}
	return TableName{normalized}, nil
}

func (tn TableName) String() string {
	return tn.Name
}

func normalizeTableName(tableName string) (string, error) {
	lowered := strings.ToLower(tableName)
	if strings.Contains(lowered, "__") {
		return "", ErrTableNameInvalid
	}
	if !tableNameChars.MatchString(lowered) {
		return "", ErrTableNameInvalid
	}
	if len(lowered) > MaxTableNameLength {
		return "", ErrTableNameTooLong
	}
	if len(lowered) < MinTableNameLength {
		return "", ErrTableNameTooShort
	}
	return lowered, nil
}
