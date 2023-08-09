package reflector_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws/awserr"
	gzip "github.com/klauspost/pgzip"
	"github.com/stretchr/testify/require"

	"github.com/segmentio/ctlstore/pkg/fakes"
	"github.com/segmentio/ctlstore/pkg/reflector"
	"github.com/segmentio/errors-go"
)

// Verifies error handling behavior when downloading from s3 fails.
// Specifically, the supervisor is allowed to create a new LDB if
// a 404 is received from S3, and other errors from S3 are able
// to be retried.  These behaviors result from the kind of error
// that s3Downloader.DownloadTo(writer) returns.
func TestS3DownloadErrors(t *testing.T) {
	for _, test := range []struct {
		name         string
		isSupervisor bool
		s3Client     func() reflector.S3Client
		n            int64
		err          error
		errTypes     []string
	}{
		{
			name: "success",
			s3Client: func() reflector.S3Client {
				f := &fakes.FakeS3Client{}
				f.GetObjectReturns(&s3.GetObjectOutput{
					Body:          ioutil.NopCloser(strings.NewReader("data")),
					ContentLength: 4,
				}, nil)
				return f
			},
			n:   4,
			err: nil,
		},
		{
			name: "failure",
			s3Client: func() reflector.S3Client {
				f := &fakes.FakeS3Client{}
				f.GetObjectReturns(nil, errors.New("failure"))
				return f
			},
			err:      errors.New("get s3 data: failure"),
			errTypes: []string{"Temporary"}, // generic failures get retried
			n:        -1,
		},
		{
			name:         "permanent failure on 404 if supervisor",
			isSupervisor: true,
			s3Client: func() reflector.S3Client {
				f := &fakes.FakeS3Client{}
				f.GetObjectReturns(nil, awserr.NewRequestFailure(
					awserr.New("error-code", "error-message", errors.New("failure")), http.StatusNotFound, ""))
				return f
			},
			err:      errors.New("failure"),
			errTypes: []string{"Permanent"},
			n:        -1,
		},
		{
			name: "temporary failure on 404 if not-supervisor",
			s3Client: func() reflector.S3Client {
				f := &fakes.FakeS3Client{}
				f.GetObjectReturns(nil, awserr.NewRequestFailure(
					awserr.New("error-code", "error-message", errors.New("failure")), http.StatusNotFound, ""))
				return f
			},
			err:      errors.New("failure"),
			errTypes: []string{"Temporary"},
			n:        -1,
		},
		{
			name: "temporary failure",
			s3Client: func() reflector.S3Client {
				f := &fakes.FakeS3Client{}
				f.GetObjectReturns(nil, awserr.NewRequestFailure(
					awserr.New("error-code", "error-message", errors.New("failure")), http.StatusInternalServerError, ""))
				return f
			},
			err:      errors.New("failure"),
			errTypes: []string{"Temporary"},
			n:        -1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := &reflector.S3Downloader{
				S3Client:            test.s3Client(),
				StartOverOnNotFound: test.isSupervisor,
			}
			var buf bytes.Buffer
			n, err := s.DownloadTo(&buf)
			if test.err == nil {
				require.NoError(t, err)
			} else {
				require.Contains(t, err.Error(), test.err.Error())
				require.EqualValues(t, len(test.errTypes), len(errors.Types(err)),
					"got types: %v", errors.Types(err))
				for _, typ := range test.errTypes {
					require.True(t, errors.Is(typ, err),
						"error did not have the error type '%s', got types: %v", typ, errors.Types(err))
				}
			}
			require.EqualValues(t, test.n, n)
		})
	}
}

// Verifies that regular and compressed snapshots are handled correctly.
func TestS3Downloader(t *testing.T) {
	for _, test := range []struct {
		name        string
		bucket      string
		key         string
		dataSize    int
		compression bool
	}{
		{
			name:        "without compression",
			bucket:      "my-bucket",
			key:         "snapshot.db",
			compression: false,
			dataSize:    1024 * 512,
		},
		{
			name:        "with compression",
			bucket:      "my-bucket",
			key:         "snapshot.db.gz",
			compression: true,
			dataSize:    1024 * 512,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := make([]byte, test.dataSize)
			_, err := rand.Read(input)
			require.NoError(t, err)

			fake := &fakes.FakeS3Client{}
			toWrite := input
			if test.compression {
				// compress the "s3 file" first
				buf := new(bytes.Buffer)
				gw := gzip.NewWriter(buf)
				_, err := io.Copy(gw, bytes.NewReader(input))
				require.NoError(t, err)
				err = gw.Close()
				require.NoError(t, err)
				toWrite = buf.Bytes()
			}
			contentLength := int64(len(toWrite))
			fake.GetObjectReturns(&s3.GetObjectOutput{
				Body:          ioutil.NopCloser(bytes.NewReader(toWrite)),
				ContentLength: contentLength,
			}, nil)

			sd := &reflector.S3Downloader{
				Bucket:   test.bucket,
				Key:      test.key,
				S3Client: fake,
			}
			w := new(bytes.Buffer)
			n, err := sd.DownloadTo(w)

			_, arg, _ := fake.GetObjectArgsForCall(0)

			require.Equal(t, test.bucket, *arg.Bucket)
			require.Equal(t, test.key, *arg.Key)
			require.NoError(t, err)
			require.EqualValues(t, len(input), n)
			require.EqualValues(t, input, w.Bytes()) // assert bytes same as input payload
		})
	}
}
