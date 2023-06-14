package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ecs"
	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/utils"
	"github.com/segmentio/errors-go"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"
)

const (
	ecsMetadataURL                 = "http://localhost:51678/v1/metadata"
	ecsContainerInstanceTargetType = "container-instance"
)

type (
	// HealthConfig configures the behavior of the container
	// instance attribute setting. Ledger latency health will be
	// reflected in container instance attributes.
	HealthConfig struct {
		DisableECSBehavior      bool          // whether or not to disable container instance attributing
		MaxHealthyLatency       time.Duration // the max latency which is considered healthy
		AttributeName           string        // the attribute name to indicate ledger latency health
		HealthyAttributeValue   string        // if ledger latency is healthy use this attribute value
		UnhealthyAttributeValue string        // if ledger latency is unhealthy use this attribute value
		PollInterval            time.Duration // how often to check for ledger latency
		AWSRegion               string        // which region to use for setting instance atts
	}
	// Monitor is the main type which performs the ledger health monitoring.
	Monitor struct {
		cfg             HealthConfig
		latencyFunc     latencyFunc         // helps us mock out querying the LDB
		ecsMetadataFunc ecsMetadataFunc     // helps us mock out querying the ECS agent
		tickerFunc      func() *time.Ticker // helps us mock out time in tests
		ecsClient       ECSClient           // helps us to mock out ECS API
		checkCallback   func()              // called when a check is done. used for testing.
	}
	latencyFunc     func(ctx context.Context) (time.Duration, error)
	ecsMetadataFunc func(ctx context.Context) (EcsMetadata, error)
	MonitorOpt      func(monitor *Monitor)
)

func NewLedgerMonitor(cfg HealthConfig, llf latencyFunc, opts ...MonitorOpt) (*Monitor, error) {
	mon := &Monitor{
		cfg:           cfg,
		latencyFunc:   llf,
		tickerFunc:    func() *time.Ticker { return time.NewTicker(cfg.PollInterval) },
		checkCallback: func() {},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(mon)
		}
	}
	return mon, nil
}

func (m *Monitor) Start(ctx context.Context) {
	events.Log("Ledger monitor starting")
	defer events.Log("Ledger monitor stopped")
	var health *bool // pointer for tri-state logic
	temporaryErrorLimit := 3
	utils.CtxFireLoopTicker(ctx, m.tickerFunc(), func() {
		defer m.checkCallback() // signal that the tick has finished
		err := func() error {
			latency, err := m.latencyFunc(ctx)
			if err != nil {
				return errors.Wrap(err, "get ledger latency")
			}
			// always instrument ledger latency even if ECS behavior is disabled.
			stats.Set("reflector-ledger-latency", latency)
			if !m.cfg.DisableECSBehavior {
				switch {
				case latency <= m.cfg.MaxHealthyLatency && (health == nil || *health != true):
					// set a healthy attribute
					if err := m.setHealthAttribute(ctx, m.cfg.HealthyAttributeValue); err != nil {
						return errors.Wrap(err, "set healthy")
					}
					health = pointer.ToBool(true)
				case latency > m.cfg.MaxHealthyLatency && (health == nil || *health != false):
					// set an unhealthy attribute
					if err := m.setHealthAttribute(ctx, m.cfg.UnhealthyAttributeValue); err != nil {
						return errors.Wrap(err, "set unhealthy")
					}
					health = pointer.ToBool(false)
				}
				switch {
				case health == nil:
					stats.Set("ledger-health", 1, stats.T("status", "unknown"))
				case *health == false:
					stats.Set("ledger-health", 1, stats.T("status", "unhealthy"))
				case *health == true:
					stats.Set("ledger-health", 1, stats.T("status", "healthy"))
				}
			}
			return nil
		}()
		switch {
		case err == nil:
		case errs.IsCanceled(err):
			// context is done, just let it fall through
		case errors.Is("temporary", err) && temporaryErrorLimit > 0:
			// don't increment error metric for a temporary error
			temporaryErrorLimit--
			events.Log("Temporary monitor ledger latency error: %s", err)
			stats.Incr("ledger-monitor-temporary-errors")
		default:
			// this is an error that must be instrumented
			events.Log("Could not monitor ledger latency: %s", err)
			errs.IncrDefault(stats.Tag{Name: "op", Value: "monitor-ledger-latency"})
		}
	})
}

func (m *Monitor) setHealthAttribute(ctx context.Context, attrValue string) error {
	events.Log("Setting ECS instance attribute: %s=%s", m.cfg.AttributeName, attrValue)
	ecsMeta, err := m.getECSMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "get ecs metadata")
	}
	clusterARN, err := m.buildClusterARN(ecsMeta)
	if err != nil {
		return errors.Wrap(err, "build cluster ARN")
	}
	events.Log("Putting attribute name=%{attName}v value=%{attValue}v targetID=%{targetID}v targetType=%{targetType}v",
		m.cfg.AttributeName, attrValue, ecsMeta.ContainerInstanceArn, ecsContainerInstanceTargetType)
	client := m.getECSClient()
	_, err = client.PutAttributes(&ecs.PutAttributesInput{
		Attributes: []*ecs.Attribute{
			{
				Name:       aws.String(m.cfg.AttributeName),
				Value:      aws.String(attrValue),
				TargetId:   aws.String(ecsMeta.ContainerInstanceArn),
				TargetType: aws.String(ecsContainerInstanceTargetType),
			},
		},
		Cluster: aws.String(clusterARN),
	})
	if err != nil {
		return errors.Wrap(err, "put attributes")
	}
	return nil
}

func (m *Monitor) getECSClient() ECSClient {
	if m.ecsClient != nil {
		return m.ecsClient
	}
	sess := session.Must(session.NewSession())
	client := ecs.New(sess)
	return client
}

func (m *Monitor) buildClusterARN(meta EcsMetadata) (arn string, err error) {
	err = func() error {
		region := m.cfg.AWSRegion
		if region == "" {
			return errors.New("no aws region available")
		}
		accountID, err := meta.accountID()
		if err != nil {
			return errors.Wrap(err, "get account id")
		}
		cluster := meta.Cluster
		arn = fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", region, accountID, cluster)
		return nil
	}()
	return arn, err
}

func (m *Monitor) getECSMetadata(ctx context.Context) (meta EcsMetadata, err error) {
	if m.ecsMetadataFunc != nil {
		return m.ecsMetadataFunc(ctx)
	}
	err = func() error {
		resp, err := http.Get(ecsMetadataURL)
		if err != nil {
			// signal that this is a temporary error and we can retry a number of times before
			// we start reporting errors.
			return errors.WithTypes(errors.Wrap(err, "get ecs metadata"), "temporary")
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return errors.Errorf("could not get ecs metadata: [%d]: %s", resp.StatusCode, b)
		}
		if err = json.NewDecoder(resp.Body).Decode(&meta); err != nil {
			return errors.Wrap(err, "read metadata")
		}
		return nil
	}()
	return meta, err
}
