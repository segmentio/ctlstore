package supervisor

import "github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"

//go:generate counterfeiter -o fakes/s3_uploader.go . S3Uploader
type S3Uploader interface {
	s3manageriface.UploaderAPI
}
