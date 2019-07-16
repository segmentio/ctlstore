package reflector

import (
	"math/rand"
	"time"
)

type jitter struct {
	Rand *rand.Rand
}

func newJitter() *jitter {
	src := rand.NewSource(time.Now().UTC().UnixNano())
	rnd := rand.New(src)
	return &jitter{rnd}
}

func (j *jitter) Jitter(dur time.Duration, coefficient float64) time.Duration {
	val := float64(dur) + (float64(dur) * (coefficient * (j.Rand.Float64() - 0.5) * 2.0))
	if val < 0.0 {
		return 0.0
	}
	return time.Duration(val)
}
