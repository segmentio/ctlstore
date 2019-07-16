package limits

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/units"
)

const (
	LimitRequestBodySize = 1 * units.MEGABYTE
	LimitMaxDMLSize      = 768 * units.KILOBYTE
	LimitFieldValueSize  = 512 * units.KILOBYTE

	LimitMaxMutateRequestCount = 100
	LimitWriterCookieSize      = 1024

	LimitWriterSecretMaxLength = 100
	LimitWriterSecretMinLength = 3
)

// TableSizeLimits is a representation of all of the table size limits
type TableSizeLimits struct {
	Global SizeLimits       `json:"global"`
	Tables []TableSizeLimit `json:"tables"`
}

// TableSizeLimit represents the limit for a particular table
type TableSizeLimit struct {
	SizeLimits
	Family string `json:"family"`
	Table  string `json:"table"`
}

// SizeLimits composes a max and a warn size
type SizeLimits struct {
	MaxSize  int64 `json:"max-size"`
	WarnSize int64 `json:"warn-size"`
}

// WriterRateLimits represents all of the writer limits
type WriterRateLimits struct {
	Global  RateLimit         `json:"global"`
	Writers []WriterRateLimit `json:"writers"`
}

// WriterRateLimit represents the limit for a particular writer
type WriterRateLimit struct {
	Writer    string    `json:"writer"`
	RateLimit RateLimit `json:"rate-limit"`
}

// RateLimit composes an amount allowed per duration
type RateLimit struct {
	Amount int64         `json:"amount"`
	Period time.Duration `json:"period"`
}

// UnmarshalJSON allows us to deser time.Durations using string values
func (l *RateLimit) UnmarshalJSON(b []byte) error {
	var val map[string]interface{}
	if err := json.Unmarshal(b, &val); err != nil {
		return err
	}
	if amount, ok := val["amount"]; ok {
		switch amount := amount.(type) {
		case float64:
			l.Amount = int64(amount)
		default:
			return errors.Errorf("invalid amount: '%v'", amount)
		}
	}
	if period, ok := val["period"]; ok {
		switch period := period.(type) {
		case float64:
			l.Period = time.Duration(int64(period))
		case string:
			parsed, err := time.ParseDuration(period)
			if err != nil {
				return errors.Errorf("invalid period: '%v'", period)
			}
			l.Period = parsed
		default:
			return errors.Errorf("invalid period: '%v'", period)
		}
	}
	return nil
}

func (l RateLimit) String() string {
	return fmt.Sprintf("%d/%v", l.Amount, l.Period)
}

// adjustAmount adjusts the composed amount for the specified period, rounded to the nearest second
func (l RateLimit) AdjustAmount(period time.Duration) (int64, error) {
	if period.Seconds() <= 0 {
		return 0, errors.New("supplied period must be positive")
	}
	scaling := l.Period.Seconds() / period.Seconds()
	amount := float64(l.Amount) / scaling
	return int64(math.RoundToEven(amount)), nil
}
