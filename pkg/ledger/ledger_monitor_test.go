package ledger_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecs"
	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/ledger"
	"github.com/segmentio/ctlstore/pkg/ledger/fakes"
	_ "github.com/segmentio/events/log"
	"github.com/stretchr/testify/require"
)

func TestLedgerMonitor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := ledger.HealthConfig{
		AttributeName:           "ctlstore-status",
		MaxHealthyLatency:       10 * time.Second,
		HealthyAttributeValue:   "healthy",
		UnhealthyAttributeValue: "unhealthy",
		AWSRegion:               "us-west-2",
	}
	ecsClient := new(fakes.FakeECSClient)
	ecsClient.PutAttributesReturns(nil, nil)
	latencyProvider := &latencyProvider{
		duration: 1 * time.Second,
	}
	ecsMetaProvider := &ecsMetaProvider{
		value: ledger.EcsMetadata{
			Cluster:              "megapool",
			ContainerInstanceArn: "arn:aws:ecs:us-west-2:12345:container-instance/container-instance-id",
		},
	}
	ft := ledger.NewFakeTicker()
	callbacks := make(chan struct{}) // every tick of the monitor will cause a send on this chan
	wait := func() {
		select {
		case <-callbacks:
		case <-ctx.Done():
		}
	}
	mon, err := ledger.NewLedgerMonitor(cfg, latencyProvider.get,
		ledger.WithTicker(ft.Ticker),
		ledger.WithECSMetadataFunc(ecsMetaProvider.get),
		ledger.WithECSClient(ecsClient),
		ledger.WithCheckCallback(func() {
			select {
			case callbacks <- struct{}{}:
			case <-ctx.Done():
			}
		}))
	require.NoError(t, err)
	require.NotNil(t, mon)
	// this validates that ECS was called to set the right attribute value
	validateAttrSet := func(attrValue string, actual *ecs.PutAttributesInput) {
		require.EqualValues(t, &ecs.PutAttributesInput{
			Attributes: []*ecs.Attribute{
				{
					Name:       aws.String("ctlstore-status"),
					TargetId:   aws.String("arn:aws:ecs:us-west-2:12345:container-instance/container-instance-id"),
					TargetType: aws.String("container-instance"),
					Value:      aws.String(attrValue),
				},
			},
			Cluster: aws.String("arn:aws:ecs:us-west-2:12345:cluster/megapool"),
		}, actual)
	}

	go mon.Start(ctx) // will do one loop before ticking

	callCount := 0

	// validate the first tick. we should be healthy.
	wait()
	callCount++
	require.Equal(t, callCount, ecsClient.PutAttributesCallCount())
	validateAttrSet("healthy", ecsClient.PutAttributesArgsForCall(callCount-1))

	// tick again. since we already set the attr to healthy we should not do so again
	ft.Tick(ctx)
	wait()
	require.Equal(t, callCount, ecsClient.PutAttributesCallCount())

	// set the ledger latency over the threshold. verify attribute set to unhealthy
	latencyProvider.setDuration(time.Hour)
	ft.Tick(ctx)
	wait()
	callCount++
	require.Equal(t, callCount, ecsClient.PutAttributesCallCount())
	validateAttrSet("unhealthy", ecsClient.PutAttributesArgsForCall(callCount-1))

	// tick again, verify that the same attr value is not set again
	ft.Tick(ctx)
	wait()
	require.Equal(t, callCount, ecsClient.PutAttributesCallCount())

	// set the latency to the threshold and verify that it becomes healthy again
	latencyProvider.setDuration(10 * time.Second)
	ft.Tick(ctx)
	wait()
	callCount++
	require.Equal(t, callCount, ecsClient.PutAttributesCallCount())
	validateAttrSet("healthy", ecsClient.PutAttributesArgsForCall(callCount-1))

	// set as latent, and produce an error on the meta provider. no call should
	// be made to ECS to set the attribute
	latencyProvider.setDuration(time.Hour)
	ecsMetaProvider.setError(errors.New("ecs failure"))
	ft.Tick(ctx)
	wait()
	require.Equal(t, callCount, ecsClient.PutAttributesCallCount())

	// clear the error, verify ECS attribute set to unhealthy
	ecsMetaProvider.setError(nil)
	ft.Tick(ctx)
	wait()
	callCount++
	require.Equal(t, callCount, ecsClient.PutAttributesCallCount())
	validateAttrSet("unhealthy", ecsClient.PutAttributesArgsForCall(callCount-1))
}

type errHolder struct {
	err error
	mut sync.Mutex
}

func (h *errHolder) setError(err error) {
	h.mut.Lock()
	defer h.mut.Unlock()
	h.err = err
}

type ecsMetaProvider struct {
	value ledger.EcsMetadata
	errHolder
}

func (p *ecsMetaProvider) get(ctx context.Context) (ledger.EcsMetadata, error) {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.value, p.err
}

func (p *ecsMetaProvider) setMeta(value ledger.EcsMetadata) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.value = value
}

type latencyProvider struct {
	duration time.Duration
	errHolder
}

func (p *latencyProvider) get(ctx context.Context) (time.Duration, error) {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.duration, p.err
}

func (p *latencyProvider) setDuration(duration time.Duration) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.duration = duration
}
