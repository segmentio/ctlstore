package supervisor

import (
	"compress/gzip"
	"io"
	"sync"

	"github.com/pkg/errors"
)

type gzipCompressionReader struct {
	reader     io.Reader      // the original reader
	pipeReader *io.PipeReader // what we'll actually read from
	bytesRead  int            // how many gzip bytes were transferred
	once       sync.Once
}

var _ io.Reader = (*gzipCompressionReader)(nil)

// newGZIPPipeReader provides a reader that reads a delegate reader's
// bytes but compresses them as GZIP. It does this by using io.Pipe()
// and a gzip writer that writes to the *PipeWriter.  The read end of
// the pipe is what is used to satisfy the io.Reader contract.
func newGZIPCompressionReader(reader io.Reader) *gzipCompressionReader {
	return &gzipCompressionReader{
		reader: reader,
	}
}

func (r *gzipCompressionReader) Read(p []byte) (n int, err error) {
	if r.reader == nil {
		return -1, errors.New("no reader specified")
	}
	r.once.Do(func() {
		var pw *io.PipeWriter
		r.pipeReader, pw = io.Pipe()
		gw := gzip.NewWriter(pw)
		go func() {
			pw.CloseWithError(func() error {
				_, err := io.Copy(gw, r.reader)
				if err != nil {
					return errors.Wrap(err, "copy to gzip writer")
				}
				if err = gw.Close(); err != nil {
					return errors.Wrap(err, "close gzip writer")
				}
				return nil
			}())
		}()
	})
	n, err = r.pipeReader.Read(p)
	if n > 0 {
		r.bytesRead += n
	}
	return n, err
}
