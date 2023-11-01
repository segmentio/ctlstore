package ledger

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTemporaryAs(t *testing.T) {
	var err error = temporaryError{errors.New("boom")}
	err = fmt.Errorf("wrapped: %w", err)
	require.True(t, errors.Is(err, temporaryError{}))
}
