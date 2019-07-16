package cmd

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetKeys(t *testing.T) {
	decodeHex := func(h string) []byte {
		res, err := hex.DecodeString(h)
		require.NoError(t, err)
		return res
	}
	for _, test := range []struct {
		name   string
		input  []string
		output []interface{}
		err    error
	}{
		{
			name:   "noargs",
			input:  []string{},
			output: nil,
			err:    nil,
		},
		{
			name:   "string arg",
			input:  []string{`foo`},
			output: []interface{}{"foo"},
			err:    nil,
		},
		{
			name:   "hex arg",
			input:  []string{"0xabcd"},
			output: []interface{}{decodeHex("abcd")},
			err:    nil,
		},
		{
			name:   "hex arg mixed case",
			input:  []string{"0xABcD"},
			output: []interface{}{decodeHex("abcd")},
			err:    nil,
		},
		{
			name:   "pass through of non-hex keys",
			input:  []string{"0xzz", "0xaz"},
			output: []interface{}{"0xzz", "0xaz"},
			err:    nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			out, err := getKeys(test.input)
			if test.err != nil {
				require.EqualError(t, err, test.err.Error())
			} else {
				require.NoError(t, err)
			}
			require.EqualValues(t, test.output, out)
		})
	}
}
