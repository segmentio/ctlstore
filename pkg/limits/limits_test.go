package limits

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimitDeser(t *testing.T) {
	for _, test := range []struct {
		desc     string
		input    map[string]interface{}
		expected RateLimit
		err      error
	}{
		{
			desc: "float amount success",
			input: map[string]interface{}{
				"amount": 15.52,
				"period": time.Minute,
			},
			expected: RateLimit{
				Amount: 15,
				Period: time.Minute,
			},
		},
		{
			desc: "nanoseconds period success",
			input: map[string]interface{}{
				"amount": 15,
				"period": time.Second.Nanoseconds(),
			},
			expected: RateLimit{
				Amount: 15,
				Period: time.Second,
			},
		},
		{
			desc: "nanoseconds small period",
			input: map[string]interface{}{
				"amount": 15,
				"period": 50,
			},
			expected: RateLimit{
				Amount: 15,
				Period: time.Duration(50),
			},
		},
		{
			desc: "string period success",
			input: map[string]interface{}{
				"amount": 15,
				"period": "10s",
			},
			expected: RateLimit{
				Amount: 15,
				Period: 10 * time.Second,
			},
		},
		{
			desc: "string period failure",
			input: map[string]interface{}{
				"amount": 15,
				"period": "10 seconds",
			},
			err: errors.New("invalid period: '10 seconds'"),
		},
		{
			desc: "invalid amount failure",
			input: map[string]interface{}{
				"amount": "foobar",
				"period": "10s",
			},
			err: errors.New("invalid amount: 'foobar'"),
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			b, err := json.Marshal(test.input)
			require.NoError(t, err)
			var res RateLimit
			err = json.Unmarshal(b, &res)
			switch {
			case err == nil && test.err != nil:
				require.Fail(t, "expected test to fail")
			case err != nil && test.err == nil:
				require.NoError(t, err)
			case err != nil && test.err != nil:
				require.EqualError(t, err, test.err.Error())
			default:
				require.EqualValues(t, test.expected, res)
			}
		})
	}
}

func TestRateLimitAdjustAmount(t *testing.T) {
	for _, test := range []struct {
		desc      string
		rateLimit RateLimit
		period    time.Duration
		expected  int64
		err       error
	}{
		{
			desc:      "1 per second to minutes",
			rateLimit: RateLimit{Amount: 1, Period: time.Second},
			period:    time.Minute,
			expected:  60,
		},
		{
			desc:      "2 per second to minutes",
			rateLimit: RateLimit{Amount: 2, Period: time.Second},
			period:    time.Minute,
			expected:  120,
		},
		{
			desc:      "60 per second to minutes",
			rateLimit: RateLimit{Amount: 60, Period: time.Second},
			period:    time.Minute,
			expected:  3600,
		},
		{
			desc:      "170 per second to minutes",
			rateLimit: RateLimit{Amount: 170, Period: time.Second},
			period:    time.Minute,
			expected:  10200,
		},
		{
			desc:      "5 per minute to minutes",
			rateLimit: RateLimit{Amount: 5, Period: time.Minute},
			period:    time.Minute,
			expected:  5,
		},
		{
			desc:      "60 per minute to seconds",
			rateLimit: RateLimit{Amount: 60, Period: time.Minute},
			period:    time.Second,
			expected:  1,
		},
		{
			desc:      "70 per minute to seconds",
			rateLimit: RateLimit{Amount: 70, Period: time.Minute},
			period:    time.Second,
			expected:  1, // round down
		},
		{
			desc:      "110 per minute to seconds",
			rateLimit: RateLimit{Amount: 110, Period: time.Minute},
			period:    time.Second,
			expected:  2, // round up
		},
		{
			desc:      "120 per minute to seconds",
			rateLimit: RateLimit{Amount: 120, Period: time.Minute},
			period:    time.Second,
			expected:  2,
		},
		{
			desc:      "divide by zero",
			rateLimit: RateLimit{Amount: 120, Period: time.Minute},
			period:    time.Duration(0),
			err:       errors.New("supplied period must be positive"),
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			res, err := test.rateLimit.AdjustAmount(test.period)
			if test.err != nil && err != nil {
				assert.EqualError(t, err, test.err.Error())
				return
			}
			if test.err == nil && err != nil {
				assert.Fail(t, err.Error())
			}
			if test.err != nil && err == nil {
				assert.Fail(t, "expected error")
			}
			assert.EqualValues(t, test.expected, res)
		})
	}
}
