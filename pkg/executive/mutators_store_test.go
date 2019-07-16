package executive

import (
	"bytes"
	"context"
	"testing"

	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestMutatorStoreExists(t *testing.T) {
	ctx := context.Background()
	db, teardown := newCtlDBTestConnection(t, "mysql")
	defer teardown()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	ms := mutatorStore{DB: tx, Ctx: ctx, TableName: "mutators"}
	checkExists := func(name string, expected bool) {
		exists, err := ms.Exists(schema.WriterName{Name: name})
		require.NoError(t, err)
		require.Equal(t, expected, exists)
	}
	checkExists("my-writer", false)
	require.NoError(t, ms.Register(schema.WriterName{Name: "my-writer"}, "my-secret"))
	checkExists("my-writer", true)
}

func TestMutatorStoreRegisterAndGet(t *testing.T) {
	ctx := context.Background()
	db, teardown := newCtlDBTestConnection(t, "mysql")
	defer teardown()

	tx1, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer tx1.Rollback()

	ms1 := mutatorStore{
		DB:        tx1,
		Ctx:       ctx,
		TableName: "mutators",
	}

	// the hash of "" is the secret that the DB that the writer is seeded with. so
	// this register should return successfully since it is a no-op
	err = ms1.Register(schema.WriterName{Name: "writer1"}, "")
	require.NoError(t, err)

	err = ms1.Register(schema.WriterName{Name: "writer1"}, "different-password")
	require.Equal(t, ErrWriterAlreadyExists, err)

	tx2, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx2.Rollback()

	ms2 := mutatorStore{
		DB:        tx2,
		Ctx:       ctx,
		TableName: "mutators",
	}

	_, ok, err := ms2.Get(schema.WriterName{Name: "writerNotFound"}, "")
	require.False(t, ok)
}

func TestMutatorStoreUpdate(t *testing.T) {
	suite := []struct {
		desc       string
		writerName string
		cookie     []byte
		ifCookie   []byte
		expectErr  error
	}{
		{
			desc:       "Overwrite existing cookie",
			writerName: "writer1",
			cookie:     []byte{0},
		},
		{
			desc:       "Check-and-set existing cookie success",
			writerName: "writer1",
			cookie:     []byte{2},
			ifCookie:   []byte{1},
		},
		{
			desc:       "Check-and-set existing cookie conflict",
			writerName: "writer1",
			cookie:     []byte{2},
			ifCookie:   []byte{0},
			expectErr:  ErrCookieConflict,
		},
		{
			desc:       "Set to same succeeds",
			writerName: "writer1",
			cookie:     []byte{1},
			ifCookie:   []byte{1},
		},
		{
			desc:       "Send super long cookie",
			writerName: "writer1",
			cookie:     bytes.Repeat([]byte{0}, 1025),
			expectErr:  ErrCookieTooLong,
		},
		{
			desc:       "Send super long if cookie",
			writerName: "writer1",
			cookie:     []byte{1},
			ifCookie:   bytes.Repeat([]byte{0}, 1025),
			expectErr:  ErrCookieTooLong,
		},
		{
			desc:       "Non-existant writer",
			writerName: "writer100",
			expectErr:  ErrWriterNotFound,
		},
	}

	for _, testCase := range suite {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx := context.Background()
			db, teardown := newCtlDBTestConnection(t, "mysql")
			defer teardown()

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer tx.Rollback()

			ms := mutatorStore{
				DB:        tx,
				Ctx:       ctx,
				TableName: "mutators",
			}

			err = ms.Update(
				schema.WriterName{Name: testCase.writerName},
				"",
				testCase.cookie,
				testCase.ifCookie)

			if want, got := testCase.expectErr, err; want != got {
				t.Errorf("Expected error %v, got %v", want, got)
			}
		})
	}

}
