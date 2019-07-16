package ledger

import "github.com/aws/aws-sdk-go/service/ecs/ecsiface"

//go:generate counterfeiter -o fakes/ecs_client.go . ECSClient
type ECSClient interface {
	ecsiface.ECSAPI
}
