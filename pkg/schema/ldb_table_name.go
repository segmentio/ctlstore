package schema

import (
	"errors"
	"strings"
)

const (
	ldbTableNameDelimiter = "___"
)

// Converts a family/table name pair to a concatenated version that works
// with SQLite, which doesn't support SQL schema objects. Use ___ to avoid
// easy accidental hijacks.
func LDBTableName(famName FamilyName, tblName TableName) string {
	return strings.Join(
		[]string{famName.Name, tblName.Name},
		ldbTableNameDelimiter)
}

// The opposite of ldbTableName()
func DecodeLDBTableName(tableName string) (fn FamilyName, tn TableName, err error) {
	splitted := strings.Split(tableName, ldbTableNameDelimiter)
	if len(splitted) != 2 {
		err = errors.New("decodeLdbTableName couldn't split string properly")
		return
	}

	fn, err = NewFamilyName(splitted[0])
	if err != nil {
		return
	}

	tn, err = NewTableName(splitted[1])
	if err != nil {
		return
	}

	return
}
