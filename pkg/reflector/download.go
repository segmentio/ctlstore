package reflector

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/errors-go"
	"github.com/segmentio/events"
)

type downloadTo interface {
	DownloadTo(w io.Writer) (int64, error)
}

type S3Downloader struct {
	Bucket              string
	Key                 string
	S3Client            S3Client
	StartOverOnNotFound bool // whether or not we should rebuild LDB if snapshot not found
}

func (d *S3Downloader) DownloadTo(w io.Writer) (n int64, err error) {
	client, err := d.getS3Client()
	if err != nil {
		return -1, err
	}
	obj, err := client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(d.Bucket),
		Key:    aws.String(d.Key),
	})
	if err != nil {
		switch err := err.(type) {
		case awserr.RequestFailure:
			if d.StartOverOnNotFound && err.StatusCode() == http.StatusNotFound {
				// don't bother retrying. we'll start with a fresh ldb.
				return -1, errors.WithTypes(errors.Wrap(err, "get s3 data"), errs.ErrTypePermanent)
			}
		}
		// retry
		return -1, errors.WithTypes(errors.Wrap(err, "get s3 data"), errs.ErrTypeTemporary)
	}
	defer obj.Body.Close()
	compressedSize := obj.ContentLength
	var reader io.Reader = obj.Body
	if strings.HasSuffix(d.Key, ".gz") {
		reader, err = gzip.NewReader(reader)
		if err != nil {
			return n, errors.Wrap(err, "create gzip reader")
		}
	}
	n, err = io.Copy(w, reader)
	if err != nil {
		return n, errors.Wrap(err, "copy from s3 to writer")
	}
	if compressedSize != nil {
		events.Log("LDB inflated %d -> %d bytes", *compressedSize, n)
	}
	return
}

func (d *S3Downloader) getS3Client() (S3Client, error) {
	if d.S3Client != nil {
		return d.S3Client, nil
	}
	sess := session.Must(session.NewSession())
	client := s3.New(sess)
	return client, nil
}

type memoryDownloader struct {
	Content []byte
}

func (d *memoryDownloader) DownloadTo(w io.Writer) (int64, error) {
	return io.Copy(w, bytes.NewReader(d.Content))
}
