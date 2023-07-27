package reflector

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/segmentio/errors-go"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"

	"github.com/segmentio/ctlstore/pkg/errs"
)

type downloadTo interface {
	DownloadTo(w io.Writer) (int64, error)
}

type S3Downloader struct {
	Region              string // optional
	Bucket              string
	Key                 string
	S3Client            S3Client
	StartOverOnNotFound bool // whether we should rebuild LDB if snapshot not found
}

func (d *S3Downloader) DownloadTo(w io.Writer) (n int64, err error) {
	client, err := d.getS3Client()
	downloader := manager.NewDownloader(client, func(d *manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024 // 64MB per part
		d.Concurrency = 5
	})
	if err != nil {
		return -1, err
	}
	start := time.Now()
	defer stats.Observe("snapshot_download_time", time.Now().Sub(start))
	buffer := manager.NewWriteAtBuffer([]byte{})
	compressedSize, err := downloader.Download(context.TODO(), buffer, &s3.GetObjectInput{
		Bucket: aws.String(d.Bucket),
		Key:    aws.String(d.Key),
	})

	//obj, err := client.GetObject(&s3.GetObjectInput{
	//	Bucket: aws.String(d.Bucket),
	//	Key:    aws.String(d.Key),
	//})
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

	events.Log("LDB inflated %d -> %d bytes", compressedSize, n)

	return
}

func (d *S3Downloader) getS3Client() (S3Client, error) {
	if d.S3Client != nil {
		return d.S3Client, nil
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(d.Region), // Empty string will result in the region value being ignored
	)

	if err != nil {
		panic(fmt.Sprintf("failed loading config, %v", err))
	}

	client := s3.NewFromConfig(cfg)
	return client, nil
}

type memoryDownloader struct {
	Content []byte
}

func (d *memoryDownloader) DownloadTo(w io.Writer) (int64, error) {
	return io.Copy(w, bytes.NewReader(d.Content))
}
