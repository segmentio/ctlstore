package schema

import (
	"sync/atomic"
	"time"
)

// These are the markers used to indicate the start and end of transactions
// in ctldb's DML log.
const DMLTxBeginKey = "--- BEGIN"
const DMLTxEndKey = "--- COMMIT"

var currentTestDmlSeq int64

type DMLSequence int64

type DMLStatement struct {
	Sequence   DMLSequence
	Timestamp  time.Time
	Statement  string
	FamilyName FamilyName
	TableName  TableName
}

func (seq DMLSequence) Int() int64 {
	return int64(seq)
}

// used for testing
func NewTestDMLStatement(statement string) DMLStatement {
	return DMLStatement{
		Statement: statement,
		Sequence:  nextTestDmlSeq(),
		Timestamp: time.Now(),
	}
}

func NewTestDMLStatementWithSharding(statement string, familyName FamilyName, tableName TableName) DMLStatement {
	return DMLStatement{
		Statement:  statement,
		Sequence:   nextTestDmlSeq(),
		Timestamp:  time.Now(),
		FamilyName: familyName,
		TableName:  tableName,
	}

}

func nextTestDmlSeq() DMLSequence {
	return DMLSequence(atomic.AddInt64(&currentTestDmlSeq, 1))
}
