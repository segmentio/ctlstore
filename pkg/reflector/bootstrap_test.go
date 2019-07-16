package reflector

import (
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/errors-go"
	"github.com/stretchr/testify/require"
)

type readerErr struct {
	r io.Reader
	e error
}

type fakeDownloadTo struct {
	res   []readerErr // a sequence of calls
	count int64
}

func (d *fakeDownloadTo) DownloadTo(w io.Writer) (int64, error) {
	re := d.res[d.count]
	d.count++
	if re.e != nil {
		return -1, re.e
	}
	return io.Copy(w, re.r)
}

func TestBoostrapLDB(t *testing.T) {
	const ldbContent = "ldb content"
	for _, test := range []struct {
		name string
		dl   downloadTo
		fc   string // what we expect in the downloaded file
		err  error
	}{
		{
			name: "success",
			dl: &fakeDownloadTo{
				res: []readerErr{
					{r: strings.NewReader(ldbContent)},
				},
			},
			fc: ldbContent,
		},
		{
			name: "failure",
			dl: &fakeDownloadTo{
				res: []readerErr{
					{e: errors.New("failure")},
				},
			},
			err: errors.New("failure"),
			fc:  ldbContent,
		},
		{
			name: "temporary failure",
			dl: &fakeDownloadTo{
				res: []readerErr{
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypeTemporary)},
					{r: strings.NewReader(ldbContent)},
				},
			},
			fc: ldbContent,
		},
		{
			name: "max temporary failures",
			dl: &fakeDownloadTo{
				res: []readerErr{
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypeTemporary)},
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypeTemporary)},
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypeTemporary)},
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypeTemporary)},
					{r: strings.NewReader(ldbContent)},
				},
			},
			fc: ldbContent,
		},
		{
			name: "too many temporary failures", // max retries edge case
			dl: &fakeDownloadTo{
				res: []readerErr{
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypeTemporary)},
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypeTemporary)},
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypeTemporary)},
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypeTemporary)},
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypeTemporary)},
					{r: strings.NewReader(ldbContent)},
				},
			},
			err: errors.New("download of ldb snapshot failed after max attempts reached: failure"),
			fc:  "",
		},
		{
			name: "permanent failure",
			dl: &fakeDownloadTo{
				res: []readerErr{
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypePermanent)},
				},
			},
			fc: "",
		},
		{
			name: "permanent failure with retries before it",
			dl: &fakeDownloadTo{
				res: []readerErr{
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypeTemporary)},
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypeTemporary)},
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypeTemporary)},
					{e: errors.WithTypes(errors.New("failure"), errs.ErrTypePermanent)},
				},
			},
			fc: "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, err := ioutil.TempFile("", "ldb.db")
			require.NoError(t, err)
			defer f.Close()
			err = bootstrapLDB(ldbBootstrapConfig{
				path:       f.Name(),
				downloadTo: test.dl,
				retryDelay: 10 * time.Millisecond,
			})
			if test.err == nil {
				require.NoError(t, err)
				b, err := ioutil.ReadFile(f.Name())
				require.NoError(t, err)
				require.EqualValues(t, test.fc, string(b))
			} else {
				require.EqualError(t, err, test.err.Error())
			}
		})
	}
}

// verifies the wrapping behavior of errors-go
func TestErrorsGoTypes(t *testing.T) {
	// verify when the outer error is typed
	err := errors.New("root cause")
	err = errors.WithTypes(err, "Temporary")
	require.True(t, errors.Is("Temporary", err))

	// verify when the inner error is typed
	err = errors.New("root cause")
	err = errors.WithTypes(err, "Temporary")
	err = errors.Wrap(err, "wrapped")
	require.True(t, errors.Is("Temporary", err))
}
