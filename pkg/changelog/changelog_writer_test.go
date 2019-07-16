package changelog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type clwWriteLineMock struct {
	Lines []string
}

func (w *clwWriteLineMock) WriteLine(s string) error {
	if w.Lines == nil {
		w.Lines = []string{}
	}
	w.Lines = append(w.Lines, s)
	return nil
}

func TestWriteChange(t *testing.T) {
	mock := &clwWriteLineMock{}
	clw := ChangelogWriter{WriteLine: mock}

	// Chose this number just to see if it serializes 54-bit integers
	// properly, because JavaScript is *INSANE*
	err := clw.WriteChange(ChangelogEntry{
		Seq:    42,
		Family: "family1",
		Table:  "table1",
		Key:    []interface{}{18014398509481984, "foo"},
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, len(mock.Lines))
	require.Equal(t, `{"seq":42,"family":"family1","table":"table1","key":[18014398509481984,"foo"]}`, mock.Lines[0])
}
