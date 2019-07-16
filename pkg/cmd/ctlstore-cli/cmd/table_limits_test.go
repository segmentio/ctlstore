package cmd

import (
	"encoding/json"
	"testing"

	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/stretchr/testify/require"
)

func TestDeserReadTableLimits(t *testing.T) {
	input := `{"global":{"max-size":104857600,"warn-size":52428800},"tables":[{"max-size":1024,"warn-size":528,"family":"loadfamily","table":"loadtable"}]}`
	var tsl limits.TableSizeLimits
	err := json.Unmarshal([]byte(input), &tsl)
	require.NoError(t, err)

	require.EqualValues(t, 104857600, tsl.Global.MaxSize)
	require.EqualValues(t, 52428800, tsl.Global.WarnSize)
	require.Len(t, tsl.Tables, 1)

	table := tsl.Tables[0]
	require.EqualValues(t, "loadfamily", table.Family)
	require.EqualValues(t, "loadtable", table.Table)
	require.EqualValues(t, 1024, table.MaxSize)
	require.EqualValues(t, 528, table.WarnSize)
}
