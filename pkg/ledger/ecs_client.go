package ledger

import "github.com/aws/aws-sdk-go/service/ecs/ecsiface"

//counterfeiter:generate -o fakes/ecs_client.go . ECSClient
type ECSClient interface {
	ecsiface.ECSAPI
}
