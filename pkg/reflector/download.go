package reflector

import (
	"bytes"
	"context"
	er "errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	gzip "github.com/klauspost/pgzip"

	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/errors-go"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"
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

	tmpdir := os.TempDir()
	file, err := os.CreateTemp(tmpdir, strings.TrimLeft(d.Key, "/"))
	if err != nil {
		return -1, errors.Wrap(err, "download snapshot into disk")
	}
	defer os.Remove(file.Name())

	start := time.Now()
	numBytes, err := downloader.Download(context.Background(), file, &s3.GetObjectInput{
		Bucket: aws.String(d.Bucket),
		Key:    aws.String(strings.TrimLeft(d.Key, "/")),
	})
	stats.Observe("snapshot_download_time", time.Now().Sub(start))

	events.Log("downloading file with key: ", d.Key)

	if err != nil {
		var ae smithy.APIError
		if er.As(err, &ae) {
			switch ae.(type) {
			case *types.NotFound:
				if d.StartOverOnNotFound {
					// don't bother retrying. we'll start with a fresh ldb.
					return -1, errors.WithTypes(errors.Wrap(err, "get s3 data"), errs.ErrTypePermanent)
				}
			}
		}
		// retry
		return -1, errors.WithTypes(errors.Wrap(err, "get s3 data"), errs.ErrTypeTemporary)
	}

	if strings.HasSuffix(d.Key, ".gz") {
		n, err = Unzip(file, w)
		if err != nil {
			return n, errors.Wrap(err, "unzip snapshot")
		}
	} else {
		n, err = io.Copy(w, file)
		if err != nil {
			return n, errors.Wrap(err, "copy snapshot")
		}
	}
	events.Log("ldb.db ready in %s seconds (s3 client)", time.Now().Sub(start))
	events.Log("LDB inflated %d -> %d bytes", numBytes, n)

	return
}

func Unzip(src io.Reader, dest io.Writer) (int64, error) {
	r, err := gzip.NewReader(src)
	if err != nil {
		return -1, err
	}
	defer r.Close()

	n, err := io.Copy(dest, r)

	if err != nil {
		return -1, err
	}

	return n, nil
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
