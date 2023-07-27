package reflector

import (
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
)

//counterfeiter:generate -o ../fakes/s3_client.go . S3Client
type S3Client interface {
	manager.DownloadAPIClient
}
