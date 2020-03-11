package supervisor

import (
	"bufio"
	"context"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/utils"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"
)

type archivedSnapshot interface {
	Upload(ctx context.Context, path string) error
}

type localSnapshot struct {
	Path string
}

func (c *localSnapshot) Upload(ctx context.Context, path string) error {
	if err := utils.EnsureDirForFile(c.Path); err != nil {
		return errors.Wrap(err, "ensure snapshot dir exists")
	}
	fdst, err := os.OpenFile(c.Path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrap(err, "opening destination file")
	}
	fsrc, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return errors.Wrap(err, "opening src file")
	}
	_, err = io.Copy(fdst, fsrc)
	if err != nil {
		return errors.Wrap(err, "copying file")
	}
	return nil
}

// sendToS3Func sends the specified content to an s3 bucket
type sendToS3Func func(ctx context.Context, key string, bucket string, body io.Reader) error

type s3Snapshot struct {
	Bucket       string
	Key          string
	sendToS3Func sendToS3Func
	s3Uploader   S3Uploader
}

func (c *s3Snapshot) Upload(ctx context.Context, path string) error {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return errors.Wrap(err, "opening file")
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return errors.Wrap(err, "stat LDB")
	}
	size := stat.Size()
	key := c.Key
	if key[0] == '/' {
		key = key[1:]
	}
	var reader io.Reader = bufio.NewReaderSize(f, 1024*32) // use a 32K buffer for reading
	var gpr *gzipCompressionReader
	if strings.HasSuffix(key, ".gz") {
		events.Log("Compressing s3 payload with GZIP")
		gpr = newGZIPCompressionReader(reader)
		reader = gpr
	}
	events.Log("Uploading %{file}s (%d bytes) to %{bucket}s/%{key}s", path, size, c.Bucket, key)
	if err = c.sendToS3(ctx, key, c.Bucket, reader); err != nil {
		return errors.Wrap(err, "send to s3")
	}
	events.Log("Successfully uploaded %{file}s to %{bucket}s/%{key}s", path, c.Bucket, key)
	if gpr != nil {
		stats.Set("ldb-size-bytes-compressed", gpr.bytesRead)
		if size > 0 {
			ratio := 1 - (float64(gpr.bytesRead) / float64(size))
			stats.Set("s3-compression-ratio", ratio)
			events.Log("Compression reduced %d -> %d bytes (%0.2f %%)", size, gpr.bytesRead, ratio*100)
		}
	}
	return nil
}

func (c *s3Snapshot) sendToS3(ctx context.Context, key string, bucket string, body io.Reader) error {
	if c.sendToS3Func != nil {
		return c.sendToS3Func(ctx, key, bucket, body)
	}
	ul, err := c.getS3Uploader()
	if err != nil {
		return err
	}
	output, err := ul.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   body,
	})
	if err == nil {
		events.Log("Wrote to S3 location: %s", output.Location)
	}
	return errors.Wrap(err, "upload with context")
}

func (c *s3Snapshot) getS3Uploader() (S3Uploader, error) {
	if c.s3Uploader != nil {
		return c.s3Uploader, nil
	}
	sess, err := session.NewSession()
	if err != nil {
		return nil, errors.Wrap(err, "creating aws session")
	}
	uploader := s3manager.NewUploader(sess)
	return uploader, nil
}

func archivedSnapshotFromURL(URL string) (archivedSnapshot, error) {
	parsed, err := url.Parse(URL)
	if err != nil {
		return nil, errors.Wrap(err, "parsing url")
	}
	switch parsed.Scheme {
	case "s3":
		events.Log("Using s3 destination for snapshots bucket=%v", parsed.Host)
		return &s3Snapshot{Bucket: parsed.Host, Key: parsed.Path}, nil
	case "file":
		events.Log("Using local FS destination for snapshots file=%v", parsed.Path)
		return &localSnapshot{parsed.Path}, nil
	default:
		return nil, errors.Errorf("Unknown scheme %s", parsed.Scheme)
	}
}
