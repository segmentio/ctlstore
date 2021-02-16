package sqlgen

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/errors-go"
)

type MetaTable struct {
	DriverName string
	FamilyName schema.FamilyName
	TableName  schema.TableName
	Fields     []schema.NamedFieldType
	KeyFields  schema.PrimaryKey
}

var fieldTypeToSQLMap = map[schema.FieldType]map[string]string{
	schema.FTString: {
		"mysql":   "VARCHAR(191)",
		"sqlite3": "VARCHAR(191)",
	},
	schema.FTInteger: {
		"mysql":   "BIGINT",
		"sqlite3": "INTEGER",
	},
	schema.FTDecimal: {
		"mysql":   "DOUBLE",
		"sqlite3": "REAL",
	},
	schema.FTText: {
		"mysql":   "MEDIUMTEXT",
		"sqlite3": "TEXT",
	},
	schema.FTBinary: {
		"mysql":   "MEDIUMBLOB",
		"sqlite3": "BLOB",
	},
	schema.FTByteString: {
		"mysql":   "VARBINARY(255)",
		"sqlite3": "BLOB(255)",
	},
}

func BuildMetaTableFromInput(
	driverName string,
	familyName string,
	tableName string,
	fieldNames []string,
	fieldTypes []schema.FieldType,
	keyFields []string,
) (famName schema.FamilyName, tblName schema.TableName, tbl *MetaTable, err error) {
	famName, err = schema.NewFamilyName(familyName)
	if err != nil {
		return
	}

	tblName, err = schema.NewTableName(tableName)
	if err != nil {
		return
	}

	nameTypeMap := make(map[string]schema.FieldType)
	fts := []schema.NamedFieldType{}
	for i, name := range fieldNames {
		var fn schema.FieldName
		fn, err = schema.NewFieldName(name)
		if err != nil {
			return
		}
		fts = append(fts, schema.NamedFieldType{Name: fn, FieldType: fieldTypes[i]})
		nameTypeMap[name] = fieldTypes[i]
	}

	keyTypes := make([]schema.FieldType, len(keyFields))
	for i, name := range keyFields {
		keyTypes[i] = nameTypeMap[name]
	}

	var pk schema.PrimaryKey
	pk, err = schema.NewPKFromRawNamesAndFieldTypes(keyFields, keyTypes)
	if err != nil {
		return
	}

	tbl = &MetaTable{
		DriverName: driverName,
		FamilyName: famName,
		TableName:  tblName,
		Fields:     fts,
		KeyFields:  pk,
	}

	return
}

var base64EncodedFieldTypes = []schema.FieldType{schema.FTByteString, schema.FTBinary}

func isBase64EncodedFieldType(fieldType schema.FieldType) bool {
	for _, eft := range base64EncodedFieldTypes {
		if fieldType == eft {
			return true
		}
	}
	return false
}

func maybeDecodeBase64(val interface{}, should bool) (interface{}, error) {
	if !should || val == nil {
		return val, nil
	}

	valstr, ok := val.(string)
	if !ok {
		return nil, errors.New("maybeDecodeBase64 input not string")
	}

	decoded, err := base64.StdEncoding.DecodeString(valstr)
	if err != nil {
		return nil, errors.Wrap(err, "maybeDecodeBase64")
	}

	return decoded, nil
}

// Return a copy of this table for a new driver
func (t MetaTable) ForDriver(newDriver string) (MetaTable, error) {
	t.DriverName = newDriver
	return t, nil
}

func (t *MetaTable) AsCreateTableDDL() (string, error) {
	tableName := schema.LDBTableName(t.FamilyName, t.TableName)
	lines := []string{}
	for _, field := range t.Fields {
		sqlType, ok := fieldTypeToSQLMap[field.FieldType][t.DriverName]
		if !ok {
			return "", fmt.Errorf("Invalid driver+type combo %s:%s", field.FieldType, t.DriverName)
		}

		line := SqlSprintf("$1 $2", dblquote(field.Name.Name), sqlType)
		lines = append(lines, line)
	}

	// TODO: should we require key fields here? or is validation a separate concern?
	pkFields := strings.Join(dblquoteStrings(t.KeyFields.Strings()), ",")
	pkDDL := SqlSprintf("PRIMARY KEY($1)", pkFields)
	lines = append(lines, pkDDL)

	tableBody := strings.Join(lines, ", ")
	q := SqlSprintf("CREATE TABLE $1 ($2);", tableName, tableBody)
	return q, nil
}

// XXX: should we validate schema with SQLite first? (yes!)
func (t *MetaTable) AddColumnDDL(fn schema.FieldName, ft schema.FieldType) (string, error) {
	ftString, ok := fieldTypeToSQLMap[ft][t.DriverName]
	if !ok {
		return "", fmt.Errorf("Invalid driver+type combo %s:%s", ft, t.DriverName)
	}

	tableName := schema.LDBTableName(t.FamilyName, t.TableName)
	ddl := SqlSprintf(
		"ALTER TABLE $1 ADD COLUMN $2 $3",
		tableName,
		dblquote(fn.Name),
		ftString)

	return ddl, nil
}

// Returns the names of the fields in this table in order
func (t *MetaTable) FieldNames() []schema.FieldName {
	fns := []schema.FieldName{}
	for _, nft := range t.Fields {
		fns = append(fns, nft.Name)
	}
	return fns
}

// Returns the DML string for an 'Upsert' for provided values
func (t *MetaTable) UpsertDML(values []interface{}) (string, error) {
	if len(values) != len(t.Fields) {
		return "", errors.New("assertion failed: len(values) != len(t.Fields)")
	}

	tableName := schema.LDBTableName(t.FamilyName, t.TableName)
	fieldNames := t.FieldNames()
	fieldNamesSQL := strings.Join(dblquoteStrings(schema.StringifyFieldNames(fieldNames)), ",")
	baseSQL := SqlSprintf("REPLACE INTO $1 ($2) VALUES(", tableName, fieldNamesSQL)

	buf := bytes.NewBuffer([]byte{})
	buf.WriteString(baseSQL)

	for i, val := range values {
		if i > 0 {
			buf.WriteString(",")
		}

		val, err := maybeDecodeBase64(val,
			isBase64EncodedFieldType(t.Fields[i].FieldType))
		if err != nil {
			return "", err
		}
		quoted, err := SQLQuote(val)
		if err != nil {
			return "", err
		}

		buf.WriteString(quoted)
	}

	buf.WriteString(")")

	return buf.String(), nil
}

// Returns the DML string for a delete for provided fields with placeholders
// for all of the key fields in proper order.
func (t *MetaTable) DeleteDML(values []interface{}) (string, error) {
	if want, got := len(values), len(t.KeyFields.Strings()); want != got {
		return "", fmt.Errorf("DeleteDML: assertion failed %v != %v", want, got)
	}
	if len(values) != len(t.KeyFields.Fields) {
		return "", errors.New("assertion failed: len(values) != len(t.KeyFields.Fields)")
	}

	tableName := schema.LDBTableName(t.FamilyName, t.TableName)
	baseSQL := SqlSprintf("DELETE FROM $1 WHERE ", tableName)

	buf := bytes.NewBuffer([]byte{})
	buf.WriteString(baseSQL)

	for i, fn := range t.KeyFields.Fields {
		if i > 0 {
			buf.WriteString(" AND ")
		}
		buf.WriteString(dblquote(fn.String()))
		buf.WriteString(" = ")

		ft, found := t.fieldTypeByName(fn)
		if !found {
			return "", errors.Errorf("DeleteDML couldn't find fieldName %s", fn.String())
		}
		val, err := maybeDecodeBase64(values[i], isBase64EncodedFieldType(ft))
		if err != nil {
			return "", err
		}
		quoted, err := SQLQuote(val)
		if err != nil {
			return "", err
		}
		buf.WriteString(quoted)
	}

	return buf.String(), nil
}

func (t *MetaTable) DropTableDDL() string {
	tableName := schema.LDBTableName(t.FamilyName, t.TableName)
	ddl := SqlSprintf(
		"DROP TABLE IF EXISTS $1",
		tableName)
	return ddl
}

func (t *MetaTable) ClearTableDDL() string {
	tableName := schema.LDBTableName(t.FamilyName, t.TableName)
	ddl := SqlSprintf(
		"DELETE FROM $1",
		tableName)

	return ddl
}

func (t *MetaTable) fieldTypeByName(fn schema.FieldName) (schema.FieldType, bool) {
	for _, x := range t.Fields {
		if x.Name == fn {
			return x.FieldType, true
		}
	}
	return 0, false
}

// Validates the schema locally and returns an error if there's a problem
func (t *MetaTable) Validate() error {
	if len(t.KeyFields.Fields) < 0 {
		return errors.New("Must have at least one primary key field")
	}

	for _, pkfn := range t.KeyFields.Fields {
		var matchingField schema.NamedFieldType
		found := false
		for _, nft := range t.Fields {
			if nft.Name == pkfn {
				found = true
				matchingField = nft
				break
			}
		}

		if !found {
			return fmt.Errorf("Primary key field '%s' not specified as a field", pkfn.Name)
		}

		if !matchingField.FieldType.CanBeKey() {
			typeName := schema.FieldTypeStringsByFieldType[matchingField.FieldType]
			return fmt.Errorf("Fields of type '%s' cannot be a key field", typeName)
		}
	}

	return nil
}

var sqlDriverNamesByType map[reflect.Type]string

// The database/sql API doesn't provide a way to get the registry name for
// a driver from the driver type.
func SqlDriverToDriverName(driver driver.Driver) string {
	if sqlDriverNamesByType == nil {
		sqlDriverNamesByType = map[reflect.Type]string{}

		for _, driverName := range sql.Drivers() {
			// Tested empty string DSN with MySQL, PostgreSQL, and SQLite3 drivers.
			db, _ := sql.Open(driverName, "")

			if db != nil {
				driverType := reflect.TypeOf(db.Driver())
				if _, ok := sqlDriverNamesByType[driverType]; !ok {
					sqlDriverNamesByType[driverType] = driverName
				}
			} else {
				// CR: wtf?
			}
		}
	}

	driverType := reflect.TypeOf(driver)
	if driverName, found := sqlDriverNamesByType[driverType]; found {
		return driverName
	}

	return ""
}

// Generates a placeholder string for N values
func SQLPlaceholderSet(count int) string {
	var buffer bytes.Buffer
	for i := 0; i < count; i++ {
		buffer.WriteString("?")
		if i != count-1 {
			buffer.WriteString(",")
		}
	}
	return buffer.String()
}

var reSQLIdentifierBegin = regexp.MustCompile("^[a-zA-Z]")
var reSQLIdentifier = regexp.MustCompile("^[_a-zA-Z0-9]+$")

const MaxSQLIdentifier = 64
const MinSQLIdentifier = 2

var ErrIdentifierTooShort = fmt.Errorf("Identifier must be at least %d characters", MinSQLIdentifier)
var ErrIdentifierTooLong = fmt.Errorf("Identifier must be at most %d characters", MaxSQLIdentifier)
var ErrIdentifierStart = errors.New("Identifier must start with a letter")
var ErrIdentifierInvalid = errors.New("Identifier can only contain letters, numbers, and underscore")

// Checks an SQL identifier (table name, column name, etc) and returns an
// error if it is an invalid identifier.
func checkSQLIdentifier(what string) error {
	if len(what) < MinSQLIdentifier {
		return ErrIdentifierTooShort
	}
	if len(what) > MaxSQLIdentifier {
		return ErrIdentifierTooLong
	}
	if !reSQLIdentifierBegin.MatchString(what) {
		return ErrIdentifierStart
	}
	if !reSQLIdentifier.MatchString(what) {
		return ErrIdentifierInvalid
	}
	return nil
}

func sqlQuoteByteArray(src []byte) (string, error) {
	// Uses "x'01234567890abcdef'" notation which works in MySQL and SQLite3
	buf := bytes.NewBuffer([]byte{})
	buf.WriteString("x'")

	enc := hex.NewEncoder(buf)
	_, err := enc.Write(src)
	if err != nil {
		return "", err
	}

	buf.WriteRune('\'')
	return buf.String(), nil
}

// Returns ANSI-SQL quoted strings for a passed type
func SQLQuote(src interface{}) (dst string, err error) {
	if src == nil {
		return "NULL", nil
	}

	switch v := src.(type) {
	case int:
		dst = strconv.FormatInt(int64(v), 10)
	case int8:
		dst = strconv.FormatInt(int64(v), 10)
	case int16:
		dst = strconv.FormatInt(int64(v), 10)
	case int32:
		dst = strconv.FormatInt(int64(v), 10)
	case int64:
		dst = strconv.FormatInt(int64(v), 10)
	case uint:
		dst = strconv.FormatUint(uint64(v), 10)
	case uint8:
		dst = strconv.FormatUint(uint64(v), 10)
	case uint16:
		dst = strconv.FormatUint(uint64(v), 10)
	case uint32:
		dst = strconv.FormatUint(uint64(v), 10)
	case uint64:
		dst = strconv.FormatUint(uint64(v), 10)
	case bool:
		dst = strconv.FormatBool(v)
	case float32:
		dst = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		dst = strconv.FormatFloat(v, 'f', -1, 64)
	case []byte:
		dst, err = sqlQuoteByteArray(v)
	case string:
		switch {
		case strings.ContainsRune(v, '\x00'):
			// if the string contains a NUL we need to encode as hex to prevent
			// sqlite3 parsing errors.
			dst, err = sqlQuoteByteArray([]byte(v))
		default:
			buf := bytes.NewBuffer([]byte{})
			buf.WriteRune('\'')
			buf.WriteString(strings.Replace(v, "'", "''", -1))
			buf.WriteRune('\'')
			dst = buf.String()
		}
	case fmt.Stringer:
		dst = v.String()
	default:
		panic(fmt.Sprintf("Can't handle type: %v", reflect.TypeOf(src)))
	}
	return
}

var sqlPrintfMatcher = regexp.MustCompile("\\$([0-9]+)")
var sqlPrintfValidValue = regexp.MustCompile("^[a-zA-Z0-9_\\(\\), \\?\"]*$")

// "Replacement" for fmt.Sprintf when splicing together SQL strings in dangerous
// ways where standard placeholders won't work, such as table and field names
func SqlSprintf(format string, args ...string) string {
	matches := sqlPrintfMatcher.FindAllStringSubmatchIndex(format, -1)
	if len(matches) > len(args) {
		panic("More placeholders than args")
	}

	hunks := []string{}
	lastRightIdx := 0
	for _, match := range matches {
		aleftIdx, arightIdx, nleftIdx, nrightIdx :=
			match[0], match[1], match[2], match[3]
		parsed, err := strconv.ParseInt(format[nleftIdx:nrightIdx], 10, 64)
		if err != nil {
			panic(err)
		}

		whichArg := int(parsed) - 1
		if whichArg > len(args) {
			panic("Placeholder $" + strconv.Itoa(whichArg) + " exceeds argument count")
		}
		if !sqlPrintfValidValue.MatchString(args[whichArg]) {
			panic("Invalid value: " + args[whichArg])
		}

		hunkLeft := format[lastRightIdx:aleftIdx]
		hunks = append(hunks, hunkLeft, args[whichArg])
		lastRightIdx = arightIdx
	}

	hunks = append(hunks, format[lastRightIdx:])
	return strings.Join(hunks, "")
}

func dblquote(str string) string {
	return fmt.Sprintf("\"%s\"", str)
}

func dblquoteStrings(strs []string) []string {
	out := []string{}
	for _, s := range strs {
		out = append(out, dblquote(s))
	}
	return out
}
