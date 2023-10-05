package supervisor

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"

	"github.com/segmentio/ctlstore/pkg/utils"
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
	s3Client     S3Client
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

	cs, err := getChecksum(path)
	if err != nil {
		return errors.Wrap(err, "generate file Checksum")
	}

	var gpr *gzipCompressionReader
	if strings.HasSuffix(key, ".gz") {
		events.Log("Compressing s3 payload with GZIP")
		gpr = newGZIPCompressionReader(reader)
		reader = gpr
	}
	events.Log("Uploading %{file}s (%d bytes) to %{bucket}s/%{key}s", path, size, c.Bucket, key)

	start := time.Now()
	if err = c.sendToS3(ctx, key, c.Bucket, reader, cs); err != nil {
		return errors.Wrap(err, "send to s3")
	}
	stats.Observe("ldb-upload-time", time.Since(start), stats.T("compressed", isCompressed(gpr)))

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

func getChecksum(path string) (string, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return "", errors.Wrap(err, "opening file")
	}
	defer f.Close()

	h := sha1.New()
	if _, err := io.Copy(h, f); err != nil {
		events.Log("failed to generate sha1 of snapshot", err)
	}

	cs := base64.StdEncoding.EncodeToString(h.Sum(nil))
	events.Log("base64 encoding of sha1: %s", cs)

	return cs, nil
}

func isCompressed(gpr *gzipCompressionReader) string {
	if gpr == nil {
		return "false"
	}
	return "true"
}

type BucketBasics struct {
	S3Client S3Client
}

func (c *s3Snapshot) sendToS3(ctx context.Context, key string, bucket string, body io.Reader, cs string) error {
	if c.sendToS3Func != nil {
		return c.sendToS3Func(ctx, key, bucket, body)
	}

	client, err := c.getS3Client()
	if err != nil {
		return err
	}

	var basics = BucketBasics{
		S3Client: client,
	}
	var partMiBs int64 = 16
	uploader := manager.NewUploader(basics.S3Client, func(u *manager.Uploader) {
		u.PartSize = partMiBs * 1024 * 1024
	})

	output, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:            &bucket,
		Key:               &key,
		Body:              body,
		ChecksumAlgorithm: "sha256",
		Metadata: map[string]string{
			"checksum": cs,
		},
	})
	if err == nil {
		events.Log("Wrote to S3 location: %s", output.Location)
	} else {
		events.Log("Couldn't upload s3 snapshot to %v:%v. Here's why: %v\n",
			bucket, key, err)
	}
	return errors.Wrap(err, "upload with context")
}

func (c *s3Snapshot) getS3Client() (S3Client, error) {
	if c.s3Client != nil {
		return c.s3Client, nil
	}
	cfg, err := config.LoadDefaultConfig(context.Background())

	if err != nil {
		panic(fmt.Sprintf("failed loading config, %v", err))
	}

	client := s3.NewFromConfig(cfg)
	return client, nil
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
