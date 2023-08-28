package ctlstore

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/stats/v4"
	"sync/atomic"
	"time"
)

// LDBRotatingReader reads data from multiple LDBs on a rotating schedule.
// The main benefit is relieving read pressure on a particular ldb file when it becomes inactive,
// allowing sqlite maintenance
type LDBRotatingReader struct {
	active         int32
	dbs            []RowRetriever
	schedule       []int8
	now            func() time.Time
	tickerInterval time.Duration
}

// RotationPeriod how many minutes each reader is active for before rotating to the next
type RotationPeriod int

const (
	// Every30 rotate on 30 minute mark in an hour
	Every30 RotationPeriod = 30
	// Every20 rotate on 20 minute marks in an hour
	Every20 RotationPeriod = 20
	// Every15 rotate on 15 minute marks in an hour
	Every15 RotationPeriod = 15
	// Every10 rotate on 10 minute marks in an hour
	Every10 RotationPeriod = 10
	// Every6 rotate on 6 minute marks in an hour
	Every6 RotationPeriod = 6

	// for simpler migration, the first ldb retains the original name
	defaultPath = DefaultCtlstorePath + ldb.DefaultLDBFilename
	ldbFormat   = DefaultCtlstorePath + "ldb-%d.db"
)

func defaultPaths(count int) []string {
	paths := []string{defaultPath}
	for i := 1; i < count; i++ {
		paths = append(paths, fmt.Sprintf(ldbFormat, i+1))
	}
	return paths
}

// RotatingReader creates a new reader that rotates which ldb it reads from on a rotation period with the default location in /var/spool/ctlstore
func RotatingReader(ctx context.Context, minutesPerRotation RotationPeriod, ldbsCount int) (RowRetriever, error) {
	return CustomerRotatingReader(ctx, minutesPerRotation, defaultPaths(ldbsCount)...)
}

// CustomerRotatingReader creates a new reader that rotates which ldb it reads from on a rotation period
func CustomerRotatingReader(ctx context.Context, minutesPerRotation RotationPeriod, ldbPaths ...string) (RowRetriever, error) {
	r, err := rotatingReader(minutesPerRotation, ldbPaths...)
	if err != nil {
		return nil, err
	}
	r.setActive()
	go r.rotate(ctx)
	return r, nil
}

func rotatingReader(minutesPerRotation RotationPeriod, ldbPaths ...string) (*LDBRotatingReader, error) {
	if len(ldbPaths) < 2 {
		return nil, errors.New("RotatingReader requires more than 1 ldb")
	}
	if !isValid(minutesPerRotation) {
		return nil, errors.New(fmt.Sprintf("invalid rotation period: %v", minutesPerRotation))
	}
	if len(ldbPaths) > 60/int(minutesPerRotation) {
		return nil, errors.New("cannot have more ldbs than rotations per hour")
	}
	var r LDBRotatingReader
	for _, p := range ldbPaths {
		reader, err := newLDBReader(p)
		if err != nil {
			return nil, err
		}
		r.dbs = append(r.dbs, reader)
	}
	r.schedule = make([]int8, 60)
	idx := 0
	for i := 1; i < 61; i++ {
		r.schedule[i-1] = int8(idx % len(ldbPaths))
		if i%int(minutesPerRotation) == 0 {
			idx++
		}
	}
	return &r, nil
}

func (r *LDBRotatingReader) setActive() {
	if r.now == nil {
		r.now = time.Now
	}
	atomic.StoreInt32(&r.active, int32(r.schedule[r.now().Minute()]))
}

// GetRowsByKeyPrefix delegates to the active LDBReader
func (r *LDBRotatingReader) GetRowsByKeyPrefix(ctx context.Context, familyName string, tableName string, key ...interface{}) (*Rows, error) {
	return r.dbs[atomic.LoadInt32(&r.active)].GetRowsByKeyPrefix(ctx, familyName, tableName, key...)
}

// GetRowByKey delegates to the active LDBReader
func (r *LDBRotatingReader) GetRowByKey(ctx context.Context, out interface{}, familyName string, tableName string, key ...interface{}) (found bool, err error) {
	return r.dbs[atomic.LoadInt32(&r.active)].GetRowByKey(ctx, out, familyName, tableName, key...)
}

// rotate by default checks every 1 minute if the active db has changed according to schedule
func (r *LDBRotatingReader) rotate(ctx context.Context) {
	if r.tickerInterval == 0 {
		r.tickerInterval = 1 * time.Minute
	}
	ticker := time.NewTicker(r.tickerInterval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			next := r.schedule[r.now().Minute()]
			if int32(next) != atomic.LoadInt32(&r.active) {
				atomic.StoreInt32(&r.active, int32(next))
				stats.Incr("rotating_reader.rotate")
			}
		}
	}
}

func isValid(rf RotationPeriod) bool {
	switch rf {
	case Every6, Every10, Every15, Every20, Every30:
		return true
	}
	return false
}
