package executive

import (
	"context"
	"fmt"
	"testing"

	"github.com/segmentio/ctlstore/pkg/schema"
)

func TestDMLLogWriterAdd(t *testing.T) {
	suite := []struct {
		desc       string
		statements []string
	}{
		{
			desc: "Insert the first statement",
			statements: []string{
				"INSERT INTO foo_bar VALUES('x', 'y', 123)",
			},
		},
	}

	for i, testCase := range suite {
		testName := fmt.Sprintf("[%d] %s", i, testCase.desc)
		statements := testCase.statements
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db, teardown := newCtlDBTestConnection(t, "mysql")
			defer teardown()

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer tx.Rollback()

			w := &dmlLedgerWriter{
				Tx:        tx,
				TableName: "ctlstore_dml_ledger",
			}
			defer w.Close()

			seqs := []schema.DMLSequence{}
			for _, stString := range statements {
				seq, err := w.Add(ctx, stString)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				seqs = append(seqs, seq)
			}

			row := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ctlstore_dml_ledger")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			var cnt int
			err = row.Scan(&cnt)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if want, got := 0, cnt; want != got {
				t.Errorf("Expected row count to start at %d, got %d", want, got)
			}

			err = tx.Commit()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			rows, err := db.QueryContext(ctx,
				"SELECT seq, statement "+
					"FROM ctlstore_dml_ledger "+
					"ORDER BY seq ASC")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer rows.Close()

			i := 0
			for rows.Next() {
				if i+1 > len(statements) {
					t.Errorf("Scanned more statements than expected")
					break
				}

				var rowSeq int64
				var rowStmt string

				err = rows.Scan(&rowSeq, &rowStmt)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				if want, got := seqs[i], schema.DMLSequence(rowSeq); want != got {
					t.Errorf("Expected %v, got %v", want, got)
				}

				if want, got := statements[i], rowStmt; want != got {
					t.Errorf("Expected %v, got %v", want, got)
				}

				i++
			}

		})
	}
}
