package schema

import (
	"strings"

	"github.com/segmentio/stats"
)

// FamilyTable composes a family name and a table name
type FamilyTable struct {
	Family string `json:"family"`
	Table  string `json:"table"`
}

// String is a Stringer implementation that produces the fully qualified table name
func (ft FamilyTable) String() string {
	return strings.Join([]string{ft.Family, ft.Table}, ldbTableNameDelimiter)
}

// Tag produces a stats tag that can be used to represent this table
func (ft FamilyTable) Tag() stats.Tag {
	return stats.Tag{Name: "table", Value: ft.String()}
}

// parseFamilyTable breaks up a full table name into family/table parts.
func ParseFamilyTable(fullName string) (ft FamilyTable, ok bool) {
	parts := strings.Split(fullName, ldbTableNameDelimiter)
	if len(parts) != 2 {
		return ft, false
	}
	ft.Family = parts[0]
	ft.Table = parts[1]
	return ft, true
}
