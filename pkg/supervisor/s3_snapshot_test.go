package supervisor

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestS3SnapshotCompression(t *testing.T) {
	for _, test := range []struct {
		name         string
		url          string
		compression  bool
		payload      string
		expectBucket string
		expectKey    string
	}{
		{
			name:         "no compression",
			url:          "s3://segment-ctlstore-snapshots-stage/snapshot.db",
			compression:  false,
			payload:      "s3 payload content",
			expectBucket: "segment-ctlstore-snapshots-stage",
			expectKey:    "snapshot.db",
		},
		{
			name:         "with compression",
			url:          "s3://segment-ctlstore-snapshots-stage/snapshot.db.gz",
			compression:  true,
			payload:      "s3 payload content",
			expectBucket: "segment-ctlstore-snapshots-stage",
			expectKey:    "snapshot.db.gz",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			snapshot, err := archivedSnapshotFromURL(test.url)
			require.NoError(t, err)
			s3snap, ok := snapshot.(*s3Snapshot)
			require.True(t, ok)
			var sent struct {
				key    string
				bucket string
				bytes  []byte
			}
			s3snap.sendToS3Func = func(ctx context.Context, key string, bucket string, body io.Reader) (err error) {
				sent.key = key
				sent.bucket = bucket
				sent.bytes, err = ioutil.ReadAll(body)
				return
			}
			file, err := ioutil.TempFile("", test.name)
			require.NoError(t, err)
			defer os.Remove(file.Name())
			_, err = io.Copy(file, strings.NewReader(test.payload))
			require.NoError(t, err)
			err = file.Close()
			require.NoError(t, err)
			err = snapshot.Upload(ctx, file.Name())
			require.NoError(t, err)
			require.Equal(t, test.expectKey, sent.key)
			require.Equal(t, test.expectBucket, sent.bucket)
			if test.compression {
				r, err := gzip.NewReader(bytes.NewReader(sent.bytes))
				require.NoError(t, err)
				b, err := ioutil.ReadAll(r)
				require.NoError(t, err)
				require.Equal(t, test.payload, string(b))
			} else {
				require.Equal(t, test.payload, string(sent.bytes))
			}
		})
	}
}
