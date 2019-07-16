package schema

// Roses are red,
// Violets are blue,
// This type would fill me with less existential dread,
// If Go had a tuple type instead
type NamedFieldType struct {
	Name      FieldName
	FieldType FieldType
}
