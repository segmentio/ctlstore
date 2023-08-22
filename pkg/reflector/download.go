package reflector

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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

type DownloadMetric struct {
	StartTime  string `json:"startTime"`
	Downloaded string `json:"downloaded"`
}

func (d *S3Downloader) DownloadTo(w io.Writer) (n int64, err error) {
	client, err := d.getS3Client()
	if err != nil {
		return -1, err
	}
	start := time.Now()
	defer func() {
		stats.Observe("snapshot_download_time", time.Now().Sub(start))
	}()
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

	_ = d.emitMetricFromFile()

	return
}

func (d *S3Downloader) emitMetricFromFile() error {
	name := "/var/spool/ctlstore/metrics.json"
	metricsFile, err := os.Open(name)
	defer func() {
		err = os.Remove(name)
	}()
	if err != nil {
		return err
	}

	defer func() {
		err = metricsFile.Close()
	}()
	if err != nil {
		return err
	}

	b, err := ioutil.ReadAll(metricsFile)
	if err != nil {
		return err
	}

	var dm DownloadMetric

	err = json.Unmarshal(b, &dm)
	if err != nil {
		return err
	}

	stats.Observe("init_snapshot_download_time", dm.StartTime, stats.Tag{
		Name:  "downloaded",
		Value: dm.Downloaded,
	})

	return nil
}

func (d *S3Downloader) getS3Client() (S3Client, error) {
	if d.S3Client != nil {
		return d.S3Client, nil
	}
	configs := []*aws.Config{}
	if d.Region != "" {
		configs = append(configs, &aws.Config{
			Region: aws.String(d.Region),
		})
	}
	sess := session.Must(session.NewSession(configs...))
	client := s3.New(sess)
	return client, nil
}

type memoryDownloader struct {
	Content []byte
}

func (d *memoryDownloader) DownloadTo(w io.Writer) (int64, error) {
	return io.Copy(w, bytes.NewReader(d.Content))
}
