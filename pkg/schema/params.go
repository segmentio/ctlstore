package schema

import (
	"fmt"

	"github.com/segmentio/ctlstore/pkg/errs"
)

func UnzipFieldsParam(fields [][]string) (fieldNames []string, fieldTypes []FieldType, err error) {
	fieldNames = []string{}
	fieldTypes = []FieldType{}
	typeMap := FieldTypeMap()

	for idx, fieldTuple := range fields {
		if want, got := 2, len(fieldTuple); want != got {
			err = &errs.BadRequestError{Err: fmt.Sprintf("Field #%d is malformed: expected %d elements, got %d", idx, want, got)}
			return
		}

		rawType := fieldTuple[1]
		mappedType, ok := typeMap[rawType]
		if !ok {
			err = &errs.BadRequestError{Err: fmt.Sprintf("Field #%d: Type '%s' unknown", idx, rawType)}
			return
		}

		fieldNames = append(fieldNames, fieldTuple[0])
		fieldTypes = append(fieldTypes, mappedType)
	}
	return
}
