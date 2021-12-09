package executive

import (
	"fmt"

	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/schema"
)

const (
	// DefaultExecutiveURL is where the executive can be reached
	DefaultExecutiveURL = "ctlstore-executive.segment.local"
)

type ExecutiveMutationRequest struct {
	TableName string
	Delete    bool
	Values    map[string]interface{}
}

//counterfeiter:generate -o fakes/executive_interface.go . ExecutiveInterface
type ExecutiveInterface interface {
	CreateFamily(familyName string) error
	CreateTable(familyName string, tableName string, fieldNames []string, fieldTypes []schema.FieldType, keyFields []string) error
	CreateTables([]schema.Table) error
	AddFields(familyName string, tableName string, fieldNames []string, fieldTypes []schema.FieldType) error

	Mutate(writerName string, writerSecret string, familyName string, cookie []byte, checkCookie []byte, requests []ExecutiveMutationRequest) error
	GetWriterCookie(writerName string, writerSecret string) ([]byte, error)
	SetWriterCookie(writerName string, writerSecret string, cookie []byte) error
	RegisterWriter(writerName string, writerSecret string) error

	TableSchema(familyName string, tableName string) (*schema.Table, error)
	FamilySchemas(familyName string) ([]schema.Table, error)

	ReadRow(familyName string, tableName string, where map[string]interface{}) (map[string]interface{}, error)

	ReadTableSizeLimits() (limits.TableSizeLimits, error)
	UpdateTableSizeLimit(limit limits.TableSizeLimit) error
	DeleteTableSizeLimit(table schema.FamilyTable) error

	ReadWriterRateLimits() (limits.WriterRateLimits, error)
	UpdateWriterRateLimit(limit limits.WriterRateLimit) error
	DeleteWriterRateLimit(writerName string) error

	ClearTable(table schema.FamilyTable) error
	DropTable(table schema.FamilyTable) error
	ReadFamilyTableNames(familyName schema.FamilyName) ([]schema.FamilyTable, error)
}

type mutationRequest struct {
	FamilyName schema.FamilyName
	TableName  schema.TableName
	Delete     bool
	Values     map[schema.FieldName]interface{}
}

func newMutationRequest(famName schema.FamilyName, req ExecutiveMutationRequest) (mutationRequest, error) {
	tblName, err := schema.NewTableName(req.TableName)
	if err != nil {
		return mutationRequest{}, nil
	}

	vals := map[schema.FieldName]interface{}{}
	for name, val := range req.Values {
		fn, err := schema.NewFieldName(name)
		if err != nil {
			return mutationRequest{}, err
		}
		vals[fn] = val
	}

	return mutationRequest{
		FamilyName: famName,
		TableName:  tblName,
		Delete:     req.Delete,
		Values:     vals,
	}, nil
}

// Returns the request Values as a slice in the order specified by the
//fieldOrder param. An error will be returned if a field is missing.
func (r *mutationRequest) valuesByOrder(fieldOrder []schema.FieldName) ([]interface{}, error) {
	values := []interface{}{}
	for _, fn := range fieldOrder {
		if v, ok := r.Values[fn]; ok {
			values = append(values, v)
		} else {
			return nil, fmt.Errorf("Missing field %s", fn)
		}
	}
	return values, nil
}

type mutationRequestSet struct {
	Requests []mutationRequest
}

func newMutationRequestSet(famName schema.FamilyName, exReqs []ExecutiveMutationRequest) (mutationRequestSet, error) {
	reqs := make([]mutationRequest, len(exReqs))
	for i, exReq := range exReqs {
		req, err := newMutationRequest(famName, exReq)
		if err != nil {
			return mutationRequestSet{}, err
		}
		reqs[i] = req
	}
	return mutationRequestSet{reqs}, nil
}

// Return the unique set of table names as a O(1) lookup map
func (s *mutationRequestSet) TableNameSet() map[schema.TableName]struct{} {
	tnset := map[schema.TableName]struct{}{}
	for _, req := range s.Requests {
		tnset[req.TableName] = struct{}{}
	}
	return tnset
}

// Return the unique set of table names as a slice
func (s *mutationRequestSet) TableNames() []schema.TableName {
	tns := []schema.TableName{}
	for tableName := range s.TableNameSet() {
		tns = append(tns, tableName)
	}
	return tns
}
