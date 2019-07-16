package reflector

import (
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

//go:generate counterfeiter -o ../fakes/s3_client.go . S3Client
type S3Client interface {
	s3iface.S3API
}
