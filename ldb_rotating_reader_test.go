package ctlstore

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/stretchr/testify/require"
	"strconv"
	"strings"
	"testing"
	"time"
)

func getMultiDBs(t *testing.T, count int) (dbs []*sql.DB, paths []string) {
	var tds []func()
	for i := 0; i < count; i++ {
		d, td, p := ldb.LDBForTestWithPath(t)
		dbs = append(dbs, d)
		tds = append(tds, td)
		paths = append(paths, p)
	}
	t.Cleanup(func() {
		for _, fn := range tds {
			fn()
		}
	})
	return dbs, paths
}

type basic struct {
	x int `ctlstore:"x"`
}

func TestBasicRotatingReader(t *testing.T) {
	dbs, paths := getMultiDBs(t, 2)
	for i, db := range dbs {
		_, err := db.Exec("CREATE TABLE family___table (x integer primary key);")
		if err != nil {
			t.Fatalf("failed to setup table: %v", err)
		}
		_, err = db.Exec(fmt.Sprintf("INSERT INTO family___table VALUES ('%d')", i+1))
		if err != nil {
			t.Fatalf("failed to insert into table: %v", err)
		}
	}

	rr, err := RotatingReader(context.Background(), Every30, paths...)
	if err != nil {
		t.Fatalf("failed to create rotating reader: %v", err)
	}

	var out basic
	found, err := rr.GetRowByKey(context.Background(), &out, "family", "table", 1)
	if err != nil || !found {
		t.Errorf("failed to find key 0: %v", err)
	}
	require.Equal(t, 1, out.x)

	reader := rr.(*LDBRotatingReader)

	var out2 basic
	reader.active.Store(1)
	found, err = reader.GetRowByKey(context.Background(), &out2, "family", "table", 2)
	if err != nil || !found {
		t.Errorf("failed to find key 1: %v", err)
	}
	require.Equal(t, 2, out2.x)

}

func TestValidRotatingReader(t *testing.T) {
	tests := []struct {
		name   string
		expErr string
		paths  []string
		rf     RotationFrequency
	}{
		{
			"1 ldb",
			"more than 1 ldb",
			[]string{"1path"},
			Every30,
		},
		{
			"No ldb",
			"more than 1 ldb",
			[]string{},
			Every30,
		},
		{
			"Nil ldb",
			"more than 1 ldb",
			nil,
			Every30,
		},
		{
			"bad rotation",
			"invalid rotation",
			[]string{"path1", "path2"},
			RotationFrequency(2),
		},
		{
			"more ldbs than freq",
			"cannot have more",
			[]string{"path1", "path2", "path3", "path4", "path5", "path6", "path7"},
			Every6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := rotatingReader(tt.rf, tt.paths...)
			if err == nil {
				t.Fatal("error expected, none found")
			}

			if !strings.Contains(err.Error(), tt.expErr) {
				t.Error("Did not find right error")
			}
		})
	}
}

func TestRotation(t *testing.T) {
	_, paths := getMultiDBs(t, 6)

	rr, err := rotatingReader(Every6, paths...)
	if err != nil {
		t.Fatal("unexpected error creating reader")
	}

	tests := []struct {
		name    string
		nowFunc func() time.Time
		exp     int
	}{
		{
			"0-5",
			func() time.Time {
				return time.Date(2023, 8, 17, 9, 1, 0, 0, time.UTC)
			},
			0,
		},
		{
			"6-11",
			func() time.Time {
				return time.Date(2023, 8, 17, 9, 8, 0, 0, time.UTC)
			},
			1,
		},
		{
			"12-17",
			func() time.Time {
				return time.Date(2023, 8, 17, 9, 17, 0, 0, time.UTC)
			},
			2,
		},
		{
			"18-23",
			func() time.Time {
				return time.Date(2023, 8, 17, 9, 21, 0, 0, time.UTC)
			},
			3,
		},
		{
			"24-29",
			func() time.Time {
				return time.Date(2023, 8, 17, 9, 24, 0, 0, time.UTC)
			},
			4,
		},
		{
			"30-35",
			func() time.Time {
				return time.Date(2023, 8, 17, 9, 32, 0, 0, time.UTC)
			},
			5,
		},
		{
			"36-41",
			func() time.Time {
				return time.Date(2023, 8, 17, 9, 41, 0, 0, time.UTC)
			},
			0,
		},
		{
			"42-47",
			func() time.Time {
				return time.Date(2023, 8, 17, 9, 42, 0, 0, time.UTC)
			},
			1,
		},
		{
			"48-53",
			func() time.Time {
				return time.Date(2023, 8, 17, 9, 53, 0, 0, time.UTC)
			},
			2,
		},
		{
			"54-59",
			func() time.Time {
				return time.Date(2023, 8, 17, 9, 59, 0, 0, time.UTC)
			},
			3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr.now = tt.nowFunc
			rr.setActive()
			if rr.active.Load() != int32(tt.exp) {
				t.Errorf("expected %d to be active, got %d instead", tt.exp, rr.active.Load())
			}
		})
	}
}

func TestMultipleReaders(t *testing.T) {
	ctx := context.Background()
	dbs, paths := getMultiDBs(t, 4)

	for i, db := range dbs {
		_, err := db.Exec("CREATE TABLE family___foo (id varchar primary key );")
		if err != nil {
			t.Fatalf("failure creating table, %v", err)
		}
		_, err = db.Exec(fmt.Sprintf("INSERT INTO family___foo values ('%d');", i))
		if err != nil {
			t.Fatalf("failure inserting into table, %v", err)
		}
	}

	rr, err := rotatingReader(Every15, paths...)
	if err != nil {
		t.Fatalf("unexpected error creating reader, %v", err)
	}
	i := 0
	wait := make(chan interface{})

	rr.now = func() time.Time {
		defer func() {
			if i != 0 {
				wait <- 1
			}
			i = i + 15
		}()
		return time.Date(2023, 8, 17, 10, 0+i, 59, 999_999_999, time.UTC)
	}
	rr.tickerInterval = 1 * time.Millisecond
	rr.setActive()
	go rr.rotate(ctx)

	for x := range dbs {
		out := make(map[string]interface{})
		val, err := rr.GetRowByKey(ctx, out, "family", "foo", x)
		if err != nil || !val {
			t.Errorf("unexpected error on GetRowByKey %v", err)
		}

		require.EqualValues(t, out, map[string]interface{}{"id": strconv.Itoa(x)}, "did not read correct value from table")

		for y := range dbs {
			if y == x {
				continue
			}
			val, err = rr.GetRowByKey(ctx, out, "family", "foo", y)
			if val || err != nil {
				t.Errorf("row with key %d should not be found", y)
			}
		}

		<-wait
		time.Sleep(500 * time.Microsecond)
	}

}

type kv struct {
	id  string `ctlstore:"id"`
	bar string `ctlstore:"bar"`
}

func TestGetRowByPrefixAfterRotation(t *testing.T) {
	ctx := context.Background()
	dbs, paths := getMultiDBs(t, 4)

	for i, db := range dbs {
		_, err := db.Exec("CREATE TABLE family___foo (id varchar, bar varchar, primary key (id, bar));")
		if err != nil {
			t.Fatalf("failure creating table, %v", err)
		}
		_, err = db.Exec(fmt.Sprintf("INSERT INTO family___foo values ('%d', '0'), ('%d', '1'), ('%d', '2'), ('%d', '3');", i, i, i, i))
		if err != nil {
			t.Fatalf("failure inserting into table, %v", err)
		}
	}

	rr, err := rotatingReader(Every15, paths...)
	if err != nil {
		t.Fatalf("unexpected error creating reader, %v", err)
	}

	i := 0
	wait := make(chan interface{})
	rr.now = func() time.Time {
		defer func() {
			if i != 0 {
				wait <- 1
			}
			i = i + 15
		}()
		return time.Date(2023, 8, 17, 10, (0+i)%60, 59, 999_999_999, time.UTC)
	}
	rr.tickerInterval = 1 * time.Millisecond
	rr.setActive()
	rows, err := rr.GetRowsByKeyPrefix(ctx, "family", "foo", "0")

	go rr.rotate(ctx)

	count := 0
	for rows.Next() {
		var tar kv
		rows.Scan(&tar)
		require.Equal(t, "0", tar.id)
		require.Equal(t, strconv.Itoa(count), tar.bar)
		<-wait
		time.Sleep(500 * time.Microsecond)
		var out kv
		count++

		// should rotate by now, check if different result set is returned
		found, err := rr.GetRowByKey(ctx, &out, "family", "foo", "0", "0")
		if count == 4 {
			require.EqualValues(t, kv{"0", "0"}, out, "should have rotated all the way back to the first reader")
		} else if found || err != nil {
			t.Errorf("should not have found the key since it rotated: %v", err)
		}
	}

	require.Equal(t, 4, count, "should've returned 4 rows")
}
