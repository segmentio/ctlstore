package supervisor

import (
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
)

//counterfeiter:generate -o fakes/s3_uploader.go . S3Uploader
type S3Client interface {
	manager.UploadAPIClient
}
