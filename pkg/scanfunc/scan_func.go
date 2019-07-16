package scanfunc

import (
	"database/sql"
	"reflect"
	"strings"

	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/unsafe"
)

const ctlTagString = "ctlstore"

type (
	// placeholder implements sql.Scanner. instances of this are used
	// as placeholder values that rows.Scan will recognize and supply
	// each column value to them.
	Placeholder struct {
		Col schema.DBColumnMeta
		Val interface{}
	}
	// scanFunc deserializes rows from the ldb into another data structure
	ScanFunc func(rows *sql.Rows) error
)

func (s *Placeholder) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		colType := strings.ToUpper(s.Col.Type)
		switch {
		case strings.HasPrefix(colType, "BLOB"):
			s.Val = src
		case strings.HasPrefix(colType, "VARBINARY"):
			s.Val = src
		default:
			// sqlite returns a []byte for string columns :-\
			// we handle this case in the default clause because
			// there are many types of string types, but only
			// one blob type in sqlite
			s.Val = string(src)
		}
	default:
		s.Val = src
	}
	return nil
}

func New(target interface{}, cols []schema.DBColumnMeta) (ScanFunc, error) {
	switch reflect.TypeOf(target).Kind() {
	case reflect.Ptr:
		return scanFuncStruct(target, cols), nil
	case reflect.Map:
		return scanFuncMap(target, cols), nil
	}
	return nil, ErrUnmarshalUnsupportedType
}

func scanFuncMap(target interface{}, cols []schema.DBColumnMeta) ScanFunc {
	return func(rows *sql.Rows) error {
		m, ok := target.(map[string]interface{})
		if m == nil || !ok {
			return ErrUnmarshalUnsupportedType
		}
		targets := make([]interface{}, 0, len(cols))

		// populate the targets with zero value types
		for _, col := range cols {
			targets = append(targets, &Placeholder{Col: col})
		}
		if err := rows.Scan(targets...); err != nil {
			return err
		}
		// look at each target and populate the map
		for i, target := range targets {
			value := target.(*Placeholder).Val
			m[cols[i].Name] = value
		}
		return nil
	}
}

func scanFuncStruct(target interface{}, cols []schema.DBColumnMeta) ScanFunc {
	return func(rows *sql.Rows) error {
		targets, err := NewUnmarshalTargetSlice(target, cols)
		if err != nil {
			return err
		}
		return rows.Scan(targets...)
	}
}

// Takes a pointer to a struct as well as a slice of column metadata, returning
// a slice of pointers which point at the tagged fields in target's type in
// the order provided in selectedCols param. This slice is for feeding as
// varargs to sql.Rows.Scan().
func NewUnmarshalTargetSlice(target interface{}, cols []schema.DBColumnMeta) ([]interface{}, error) {
	// prevents panics below!
	if reflect.TypeOf(target).Kind() != reflect.Ptr {
		return nil, ErrUnmarshalUnsupportedType
	}

	targetVal := reflect.ValueOf(target).Elem()
	targetType := targetVal.Type()

	// TODO: check for unexported FIELDS, not types

	buildMeta := func(typ reflect.Type) (UnmarshalTypeMeta, error) {
		// Only supports structs!
		if targetType.Kind() != reflect.Struct {
			return UnmarshalTypeMeta{}, ErrUnmarshalUnsupportedType
		}
		// Reads the field type information to extract the tags, which are used
		// to map the struct fields to column names. It then builds a map indexed
		// by the column name which references the field metadata, tying them
		// together for later use.
		fields := map[string]UnmarshalTypeMetaField{}
		for i := 0; i < targetType.NumField(); i++ {
			field := targetType.Field(i)
			tagVal, found := field.Tag.Lookup(ctlTagString)
			if found {
				tagVal = strings.ToLower(tagVal)
				fields[tagVal] = UnmarshalTypeMetaField{
					Field:   field,
					Factory: unsafe.NewInterfaceFactory(field.Type),
				}
			}
		}
		return UnmarshalTypeMeta{
			Fields: fields,
		}, nil
	}

	meta, err := UtcCache.GetOrSet(targetType, buildMeta)
	if err != nil {
		return nil, err
	}

	//
	// Construct a slice of pointers which point to the fields in the struct
	// itself. It uses cached metadata for the type information on each field.
	//
	// The static equivalent would be:
	//   target[0] = &struct.field1
	//   target[1] = &struct.field2
	//
	// This slice can then be passed to functions like database.sql.Rows.Scan,
	// which need pointers to the fields themselves to fill out a struct.
	//
	// In some cases, fields will be missing, so the target pointer points
	// to the value of a type which implements Scanner, but does nothing,
	// or the "no-op" scanner.
	//
	targets := make([]interface{}, len(cols))
	for i, col := range cols {
		colName := col.Name
		var elem interface{} = &UtcNoopScanner
		if fieldMeta, ok := meta.Fields[colName]; ok {
			elem = fieldMeta.Factory.PtrToStructField(target, fieldMeta.Field)
		}
		targets[i] = elem
	}
	return targets, nil
}
