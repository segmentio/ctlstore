package schema

import (
	"strings"
)

type FieldType int

// CanBeKey returns if the field type can be used in a PK
func (ft FieldType) CanBeKey() bool {
	return ft == FTString || ft == FTInteger || ft == FTByteString
}

func (ft FieldType) String() string {
	if s, ok := FieldTypeStringsByFieldType[ft]; ok {
		return s
	}

	return "unknown"
}

const (
	_ FieldType = iota
	FTString
	FTInteger
	FTDecimal
	FTText
	FTBinary
	FTByteString
)

// Maps FieldTypes to their stringly typed version
var FieldTypeStringsByFieldType = map[FieldType]string{
	FTString:     "string",
	FTInteger:    "integer",
	FTDecimal:    "decimal",
	FTText:       "text",
	FTBinary:     "binary",
	FTByteString: "bytestring",
}

// Used for converting SQL-ized field types to FieldTypes
var _sqlTypesToFieldTypes = map[string]FieldType{
	"varchar":      FTString,
	"varchar(191)": FTString,
	"char":         FTString,
	"character":    FTString,

	"text":       FTText,
	"mediumtext": FTText,
	"longtext":   FTText,

	"integer":   FTInteger,
	"smallint":  FTInteger,
	"mediumint": FTInteger,
	"bigint":    FTInteger,

	"real":   FTDecimal,
	"float":  FTDecimal,
	"double": FTDecimal,

	"blob":       FTBinary,
	"mediumblob": FTBinary,
	"longblob":   FTBinary,

	"varbinary": FTByteString,
	"blob(255)": FTByteString,
}

// Convert a known SQL type string to a FieldType
func SqlTypeToFieldType(sqlType string) (FieldType, bool) {
	// TODO: write a test that resolves all known generated types against this one
	loweredType := strings.ToLower(sqlType)
	ft, ok := _sqlTypesToFieldTypes[loweredType]
	return ft, ok
}

// Returns a map of stringly typed field types to strongly typed field types
func FieldTypeMap() map[string]FieldType {
	ftm := map[string]FieldType{}
	for ft, str := range FieldTypeStringsByFieldType {
		ftm[str] = ft
	}
	return ftm
}
