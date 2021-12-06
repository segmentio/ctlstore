package ledger

import (
	"fmt"
	"strings"
)

type EcsMetadata struct {
	ContainerInstanceArn string
	Cluster              string
}

// accountID parses the container instance arn and returns the account id portion
//
// ex: "arn:aws:ecs:us-west-2:[accountId]:container-instance/[instance-id]"
func (m EcsMetadata) accountID() (string, error) {
	parts := strings.Split(m.ContainerInstanceArn, ":")
	if len(parts) != 6 {
		return "", fmt.Errorf("invalid container instance arn: '%s'", m.ContainerInstanceArn)
	}
	return parts[4], nil
}
