package cmd

import (
	"path"
	"strings"

	"github.com/segmentio/ctlstore"
	"github.com/segmentio/ctlstore/pkg/ldb"
)

type flagBase struct {
}

type flagRowsPerMinute struct {
	RowsPerMinute int64 `flag:"rows-per-minute"`
}

type flagWriter struct {
	Writer string `flag:"writer"`
}

func (f flagWriter) MustWriter() string {
	if f.Writer == "" {
		bail("Writer required")
	}
	return f.Writer
}

type flagQuiet struct {
	Quiet bool `flag:"-q,--quiet"`
}

type flagExecutive struct {
	Executive string `flag:"-e,--executive" default:"ctlstore-executive.segment.local"`
}

func (f flagExecutive) MustExecutive() string {
	return normalizeURL(f.Executive)
}

type flagFamily struct {
	Family string `flag:"-f,--family"`
}

func (f flagFamily) MustFamily() string {
	if f.Family == "" {
		bail("Family required")
	}
	return f.Family
}

type flagTable struct {
	Table string `flag:"-t,--table"`
}

func (f flagTable) MustTable() string {
	if f.Table == "" {
		bail("Table required")
	}
	return f.Table
}

type flagSizeLimits struct {
	MaxSize  int64 `flag:"--max-size" default:"104857600"`
	WarnSize int64 `flag:"--warn-size" default:"52428800"`
}

func (f flagSizeLimits) MustMaxSize() int64 {
	switch {
	case f.MaxSize < 0:
		bail("Max size cannot be negative")
	case f.MaxSize == 0:
		bail("Max size required")
	}
	return f.MaxSize
}

func (f flagSizeLimits) MustWarnSize() int64 {
	switch {
	case f.WarnSize < 0:
		bail("Warn size cannot be negative")
	case f.WarnSize == 0:
		bail("Warn size required")
	}
	return f.WarnSize
}

type flagFields struct {
	Fields []string `flag:"--field"`
}

func (f flagFields) MustFields() (res []field) {
	for _, val := range f.Fields {
		parts := strings.Split(val, ":")
		if len(parts) != 2 {
			bail("invalid field: %s", val)
		}
		res = append(res, field{
			name: parts[0],
			typ:  parts[1],
		})
	}
	return
}

type flagKeyFields struct {
	KeyFields []string `flag:"--key-field"`
}

func (f flagKeyFields) MustKeyFields() []string {
	return f.KeyFields
}

type flagLDBPath struct {
	LDBPath string `flag:"-l,--ldb" default:"/var/spool/ctlstore/ldb.db"`
}

var (
	defaultLDBPath = path.Join(ctlstore.DefaultCtlstorePath, ldb.DefaultLDBFilename)
)
