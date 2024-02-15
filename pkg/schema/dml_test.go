package schema

import (
	"reflect"
	"testing"
)

func TestDMLSequence_Int(t *testing.T) {
	tests := []struct {
		name string
		seq  DMLSequence
		want int64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.seq.Int(); got != tt.want {
				t.Errorf("Int() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewTestDMLStatement(t *testing.T) {
	type args struct {
		statement string
	}
	tests := []struct {
		name string
		args args
		want DMLStatement
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewTestDMLStatement(tt.args.statement); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewTestDMLStatement() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_nextTestDmlSeq(t *testing.T) {
	tests := []struct {
		name string
		want DMLSequence
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nextTestDmlSeq(); got != tt.want {
				t.Errorf("nextTestDmlSeq() = %v, want %v", got, tt.want)
			}
		})
	}
}
