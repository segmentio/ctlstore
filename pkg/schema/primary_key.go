package schema

import errors "github.com/segmentio/errors-go"

var PrimaryKeyZero = PrimaryKey{}

type PrimaryKey struct {
	Fields []FieldName
	Types  []FieldType
}

// Is this a zero value?
func (pk *PrimaryKey) Zero() bool {
	return len(pk.Fields) == 0
}

// Returns the list of fields as strings
func (pk *PrimaryKey) Strings() []string {
	out := make([]string, len(pk.Fields))
	for i, fn := range pk.Fields {
		out[i] = fn.Name
	}
	return out
}

// builds a new primary key from a slice of field names and the corresponding field types.
func NewPKFromRawNamesAndFieldTypes(names []string, types []FieldType) (PrimaryKey, error) {
	fns := make([]FieldName, len(names))
	for i, name := range names {
		fn, err := NewFieldName(name)
		if err != nil {
			return PrimaryKeyZero, err
		}
		fns[i] = fn
	}
	return PrimaryKey{Fields: fns, Types: types}, nil
}

// builds a new primary key from a slice of field name and the string representation of
// the field types.  The field types in this case should have entries in the
// _sqlTypesToFieldTypes map.  A failure to map from a field type string to a FieldType
// will result in an error.
func NewPKFromRawNamesAndTypes(names []string, types []string) (PrimaryKey, error) {
	fns := make([]FieldName, len(names))
	fts := make([]FieldType, len(names))
	for i, name := range names {
		fn, err := NewFieldName(name)
		if err != nil {
			return PrimaryKeyZero, err
		}
		ft, ok := SqlTypeToFieldType(types[i])
		if !ok {
			return PrimaryKeyZero, errors.Errorf("no field type found for '%s'", types[i])
		}
		fns[i] = fn
		fts[i] = ft
	}
	return PrimaryKey{Fields: fns, Types: fts}, nil
}
