package ctlstore_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/segmentio/ctlstore"
)

func TestGetRowByKeyExternalPackage(t *testing.T) {
	type testKVStruct struct {
		Key string `ctlstore:"key"`
		Val string `ctlstore:"value"`
	}

	const initSQL = `
CREATE TABLE family1___table1 (
	key VARCHAR PRIMARY KEY,
	value VARCHAR
);

INSERT INTO family1___table1 VALUES('foo', 'bar');
`
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Unexpected error %+v", err)
	}
	_, err = db.Exec(initSQL)
	if err != nil {
		t.Fatalf("Unexpected error +%v", err)
	}

	reader := ctlstore.NewLDBReaderFromDB(db)
	gotOut := testKVStruct{}
	_, gotErr := reader.GetRowByKey(
		ctx,
		&gotOut,
		"family1",
		"table1",
		"foo",
	)

	if gotErr != nil {
		t.Errorf("Unexpected error %+v", gotErr)
	}

	if diff := cmp.Diff(gotOut, testKVStruct{"foo", "bar"}); diff != "" {
		t.Errorf("GetRowByKey out param mismatch\n%s", diff)
	}
}
