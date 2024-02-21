package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/segmentio/conf"
	"github.com/segmentio/errors-go"
	"github.com/segmentio/events/v2"
	_ "github.com/segmentio/events/v2/sigevents"
	"github.com/segmentio/stats/v4"
	"github.com/segmentio/stats/v4/datadog"
	"github.com/segmentio/stats/v4/procstats"
	"github.com/segmentio/stats/v4/prometheus"

	"github.com/segmentio/ctlstore"
	"github.com/segmentio/ctlstore/pkg/ctldb"
	"github.com/segmentio/ctlstore/pkg/errs"
	executivepkg "github.com/segmentio/ctlstore/pkg/executive"
	heartbeatpkg "github.com/segmentio/ctlstore/pkg/heartbeat"
	"github.com/segmentio/ctlstore/pkg/ldbwriter"
	"github.com/segmentio/ctlstore/pkg/ledger"
	reflectorpkg "github.com/segmentio/ctlstore/pkg/reflector"
	sidecarpkg "github.com/segmentio/ctlstore/pkg/sidecar"
	supervisorpkg "github.com/segmentio/ctlstore/pkg/supervisor"
	"github.com/segmentio/ctlstore/pkg/units"
	"github.com/segmentio/ctlstore/pkg/utils"
)

var DebugEnabled = false

type dogstatsdConfig struct {
	Address    string        `conf:"address" help:"Address of the dogstatsd agent that will receive metrics"`
	BufferSize int           `conf:"buffer-size" help:"Size of the statsd metrics buffer" validate:"min=0"`
	FlushEvery time.Duration `conf:"flush-every" help:"Flush AT LEAST this frequently"`
}

type sidecarConfig struct {
	BindAddr    string          `conf:"bind-addr" help:"The address and port to bind on"`
	LDBPath     string          `conf:"ldb-path" help:"The location of the LDB"`
	MaxRows     int             `conf:"max-rows" help:"Maximum number of rows that can be returned in one response"`
	Application string          `conf:"application" help:"The name of the application that will be using the sidecar"`
	Dogstatsd   dogstatsdConfig `conf:"dogstatsd" help:"dogstatsd Configuration"`
}

type reflectorCliConfig struct {
	LDBPath                    string                   `conf:"ldb-path" help:"Path to LDB file" validate:"nonzero"`
	ChangelogPath              string                   `conf:"changelog-path" help:"Path to changelog file"`
	ChangelogSize              int                      `conf:"changelog-size" help:"Maximum size of the changelog file"`
	UpstreamDriver             string                   `conf:"upstream-driver" help:"Upstream driver name (e.g. sqlite3)" validate:"nonzero"`
	UpstreamDSN                string                   `conf:"upstream-dsn" help:"Upstream DSN (e.g. path to file if sqlite3)" validate:"nonzero"`
	UpstreamLedgerTable        string                   `conf:"upstream-ledger-table" help:"Table on the upstream to look for statement ledger"`
	UpstreamShardingFamily     string                   `conf:"upstream-sharding-family" help:"Sharding family(s) reflector is targeting"`
	UpstreamShardingTable      string                   `conf:"upstream-sharding-table" help:"Sharding tables(s) reflector is targeting"`
	BootstrapURL               string                   `conf:"bootstrap-url" help:"Bootstraps LDB from an S3 URL"`
	BootstrapRegion            string                   `conf:"bootstrap-region" help:"If specified, indicates which region in which the S3 bucket lives"`
	PollInterval               time.Duration            `conf:"poll-interval" help:"How often to pull the upstream" validate:"nonzero"`
	PollJitterCoefficient      float64                  `conf:"poll-jitter-coefficient" help:"Coefficient for poll jittering"`
	PollTimeout                time.Duration            `conf:"poll-timeout" help:"How long to poll from the source before canceling"`
	QueryBlockSize             int                      `conf:"query-block-size" help:"Number of ledger entries to get at once"`
	Debug                      bool                     `conf:"debug" help:"Turns on debug logging"`
	LedgerHealth               ledgerHealthConfig       `conf:"ledger-latency" help:"Configure ledger latency behavior"`
	Dogstatsd                  dogstatsdConfig          `conf:"dogstatsd" help:"dogstatsd Configuration"`
	MetricsBind                string                   `conf:"metrics-bind" help:"address to serve Prometheus metircs"`
	WALPollInterval            time.Duration            `conf:"wal-poll-interval" help:"How often to pull the sqlite's wal size and status. 0 indicates disabled monitoring'"`
	WALCheckpointThresholdSize int                      `conf:"wal-checkpoint-threshold-size" help:"Performs a checkpoint after the WAL file exceeds this size in bytes"`
	WALCheckpointType          ldbwriter.CheckpointType `conf:"wal-checkpoint-type" help:"what type of checkpoint to manually perform once the wal size is exceeded"`
	BusyTimeoutMS              int                      `conf:"busy-timeout-ms" help:"Set a busy timeout on the connection string for sqlite in milliseconds"`
	MultiReflector             multiReflectorConfig     `conf:"multi-reflector" help:"Configuration for running multiple reflectors at once"`
}

type multiReflectorConfig struct {
	LDBPaths []string `conf:"ldb-paths" help:"list of ldbs, each ldb is managed by a unique reflector"`
}

type executiveCliConfig struct {
	Bind                           string          `conf:"bind" help:"Address for binding the HTTP server" validate:"nonzero"`
	CtlDBDSN                       string          `conf:"ctldb" help:"SQL DSN for ctldb" validate:"nonzero"`
	Debug                          bool            `conf:"debug" help:"Turns on debug logging"`
	HandlerTimeout                 time.Duration   `conf:"handler-timeout" help:"Timeout on request handling"`
	MaxTableSize                   int64           `conf:"max-table-size" help:"Max table size in bytes"`
	WarnTableSize                  int64           `conf:"warn-table-size" help:"Emit a metric when a table sizes grows past this threshold"`
	WriterLimitPeriod              time.Duration   `conf:"writer-limit-period" help:"The period to use for writer-limit"`
	WriterLimit                    int64           `conf:"writer-limit" help:"How many rows a writer may mutate per period"`
	Shadow                         bool            `conf:"shadow" help:"set this to true to emit shadow=true metric tags"`
	Dogstatsd                      dogstatsdConfig `conf:"dogstatsd" help:"dogstatsd Configuration"`
	EnableDestructiveSchemaChanges bool            `conf:"enable-destructive-schema-changes" help:"Turns on the ability to clear and drop tables from the executive API"`
}

// supervisorCliConfig also composes a reflectorCliConfig because it ends up
// running its own reflector.  The LDBPath will come from the composed
// reflector config instead of being a top level element in this struct.
type supervisorCliConfig struct {
	SnapshotInterval    time.Duration      `conf:"snapshot-interval" help:"Wait time between snapshots" validate:"nonzero"`
	SnapshotURL         string             `conf:"snapshot-url" help:"URL for snapshot upload (i.e. s3://bucket/key)" validate:"nonzero"`
	Debug               bool               `conf:"debug" help:"Turns on debug logging"`
	LedgerLatencyConfig ledgerHealthConfig `conf:"ledger-latency-health" help:"Configures ledger latency health behavior"`
	ReflectorConfig     reflectorCliConfig `conf:"reflector" help:"reflector configuration"`
	Shadow              bool               `conf:"shadow" help:"set this to true to emit shadow=true metric tags"`
	Dogstatsd           dogstatsdConfig    `conf:"dogstatsd" help:"dogstatsd Configuration"`
}

// ledgerHealthConfig configures the behavior of the container
// instance attribute tagging. Ledger latency health will be
// reflected in container instance attributes.
type ledgerHealthConfig struct {
	Disable                 bool          `conf:"disable" help:"disable ledger latency health attributing (DEPRECATED: use disable-ecs-behavior instead)"`
	DisableECSBehavior      bool          `conf:"disable-ecs-behavior" help:"disable ledger latency health attributing"`
	MaxHealthyLatency       time.Duration `conf:"max-healty-latency" help:"Max latency considered healthy"`
	AttributeName           string        `conf:"attribute-name" help:"The name of the attribute"`
	HealthyAttributeValue   string        `conf:"healthy-attribute-value" help:"The value of the attribute if healthy"`
	UnhealthyAttributeValue string        `conf:"unhealth-attribute-value" help:"The value of the attribute if unhealthy"`
	PollInterval            time.Duration `conf:"poll-interval" help:"How frequently the ledger health should be checked"`
	AWSRegion               string        `conf:"aws-region" help:"The AWS region to use"`
}

type heartbeatCliConfig struct {
	HeartbeatInterval time.Duration   `conf:"heartbeat-interval" help:"Wait time between heartbeats" validate:"nonzero"`
	ExecutiveURL      string          `conf:"executive-url" help:"URL for the executive API" validate:"nonzero"`
	FamilyName        string          `conf:"family-name" help:"The family name" validate:"nonzero"`
	TableName         string          `conf:"table-name" help:"The table name" validate:"nonzero"`
	WriterName        string          `conf:"writer-name" help:"Writer name" validate:"nonzero"`
	WriterSecret      string          `conf:"writer-secret" help:"Writer secret" validate:"nonzero"`
	Debug             bool            `conf:"debug" help:"Turns on debug logging"`
	Dogstatsd         dogstatsdConfig `conf:"dogstatsd" help:"dogstatsd Configuration"`
}

type ldbReadKeyParams struct {
	LDBPath string `conf:"ldb-path" help:"Path to LDB file" validate:"nonzero"`
	Family  string `conf:"family" validate:"nonzero"`
	Table   string `conf:"table" validate:"nonzero"`
	KeyJSON string `conf:"key-json" help:"Key as a JSON-encoded array" validate:"nonzero"`
}

func loadConfig(config interface{}, name string, args []string, help ...string) {
	var usage string

	if len(help) != 0 {
		usage = strings.Join(help, " ")
	}

	conf.LoadWith(config, conf.Loader{
		Name:  "ctlstore " + name,
		Args:  args,
		Usage: usage,
		Sources: []conf.Source{
			conf.NewEnvSource("CTLSTORE", os.Environ()...),
		},
	})
}

func main() {
	ld := conf.Loader{
		Name: "ctlstore",
		Args: os.Args[1:],
		Commands: []conf.Command{
			{Name: "version", Help: "Get the ctlstore version"},
			{Name: "reflector", Help: "Run the ctlstore Reflector"},
			{Name: "multi-reflector", Help: "Run the ctlstore Reflector in multi mode"},
			{Name: "sidecar", Help: "Run the ctlstore Sidecar"},
			{Name: "executive", Help: "Run the ctlstore Executive service"},
			{Name: "supervisor", Help: "Run the ctlstore Supervisor service"},
			{Name: "heartbeat", Help: "Run the ctlstore Heartbeat service"},
			{Name: "ldb-read-key", Help: "Reads a key from the LDB"},
			{Name: "ctldb-schema", Help: "Dump the MySQL schema for the CtlDB"},
		},
	}

	ctx, cancel := events.WithSignals(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	events.DefaultLogger.EnableDebug = false

	switch cmd, args := conf.LoadWith(nil, ld); cmd {
	case "version":
		fmt.Println(ctlstore.Version)
	case "reflector":
		reflector(ctx, args)
	case "multi-reflector":
		multiReflector(ctx, args)
	case "sidecar":
		sidecar(ctx, args)
	case "executive":
		executive(ctx, args)
	case "supervisor":
		supervisor(ctx, args)
	case "heartbeat":
		heartbeat(ctx, args)
	case "ctldb-schema":
		ctldbSchema(ctx, args)
	case "ldb-read-key":
		ldbReadKey(ctx, args)
	default:
		panic("inconceivable")
	}
}

func enableDebug() {
	events.DefaultLogger.EnableDebug = true
	events.DefaultLogger.EnableSource = true
	DebugEnabled = true
}

func defaultDogstatsdConfig() dogstatsdConfig {
	return dogstatsdConfig{
		BufferSize: 1024,
		FlushEvery: 5 * time.Second,
	}
}

type dogstatsdOpts struct {
	config            dogstatsdConfig
	statsPrefix       string
	defaultTags       []stats.Tag
	defaultTagFilters []string
	prometheusHandler *prometheus.Handler
}

func configureDogstatsd(ctx context.Context, opts dogstatsdOpts) (dd *datadog.Client, teardown func()) {
	config := opts.config
	if opts.statsPrefix == "" {
		panic("configureDogstatsd: Invalid statsPrefix passed. Stop.")
	}

	if config.Address != "" {
		dd = datadog.NewClientWith(datadog.ClientConfig{
			Address:    config.Address,
			BufferSize: config.BufferSize,
			Filters:    opts.defaultTagFilters,
		})
		stats.Register(dd)

		events.Log("Setup dogstatsd with addr:%{addr}s, buffersize:%{buffersize}d, prefix:%{pfx}s, version:%{version}s",
			config.Address, config.BufferSize, opts.statsPrefix, ctlstore.Version)
	}

	if opts.prometheusHandler != nil {
		stats.Register(opts.prometheusHandler)
	}

	if stats.DefaultEngine.Handler != stats.Discard {
		stats.DefaultEngine.Prefix = fmt.Sprintf("ctlstore.%s", opts.statsPrefix)
		stats.DefaultEngine.Tags = append(stats.DefaultEngine.Tags, stats.Tag{Name: "version", Value: ctlstore.Version})
		for _, t := range opts.defaultTags {
			stats.DefaultEngine.Tags = append(stats.DefaultEngine.Tags, t)
		}
		stats.DefaultEngine.Tags = stats.SortTags(stats.DefaultEngine.Tags) // tags must be sorted

		c := procstats.StartCollector(procstats.NewGoMetrics())

		go utils.CtxLoop(ctx, config.FlushEvery, stats.Flush)
		return dd, func() {
			c.Close()
			stats.Flush()
		}
	}
	// nothing to be done for teardown here
	return dd, func() {}
}

func ldbReadKey(_ context.Context, args []string) {
	cliParams := ldbReadKeyParams{}
	loadConfig(&cliParams, "ldb-read-key", args)

	_, err := ctlstore.ReaderForPath(cliParams.LDBPath)
	if err != nil {
		fmt.Printf("Error opening reader: %+v\n", err)
		return
	}

	fmt.Printf("Not yet implemented\n")
}

func ctldbSchema(_ context.Context, _ []string) {
	fmt.Printf("%s\n", ctldb.CtlDBSchemaByDriver["mysql"])
}

func supervisor(ctx context.Context, args []string) {
	err := func() error {
		reflectorConfig := defaultReflectorCLIConfig(true)
		cliCfg := supervisorCliConfig{
			SnapshotInterval: 5 * time.Minute,
			Dogstatsd:        defaultDogstatsdConfig(),
			ReflectorConfig:  reflectorConfig,
		}
		loadConfig(&cliCfg, "supervisor", args)
		if cliCfg.Debug {
			enableDebug()
		}

		shadow := "false"
		if cliCfg.Shadow {
			shadow = "true"
		}

		_, teardown := configureDogstatsd(ctx, dogstatsdOpts{
			config:      cliCfg.Dogstatsd,
			statsPrefix: "supervisor",
			defaultTags: []stats.Tag{stats.T("shadow", shadow)},
		})
		defer teardown()
		if err := utils.EnsureDirForFile(cliCfg.ReflectorConfig.LDBPath); err != nil {
			return errors.Wrap(err, "ensure ldb dir")
		}

		reflector, err := newReflector(cliCfg.ReflectorConfig, true, 0)
		if err != nil {
			return errors.Wrap(err, "build supervisor reflector")
		}

		supervisor, err := supervisorpkg.SupervisorFromConfig(supervisorpkg.SupervisorConfig{
			SnapshotInterval: cliCfg.SnapshotInterval,
			SnapshotURL:      cliCfg.SnapshotURL,
			LDBPath:          cliCfg.ReflectorConfig.LDBPath, // use the reflector config's ldb path here
			Reflector:        reflector,                      // compose the reflector, since it will start with the supervisor
		})
		if err != nil {
			return errors.Wrap(err, "start supervisor")
		}
		defer supervisor.Close()
		supervisor.Start(ctx)
		return nil
	}()
	if err != nil && !errs.IsCanceled(err) {
		events.Log("Fatal Supervisor error: %{error}+v", err)
		errs.IncrDefault(stats.T("op", "startup"))
	}
}

func heartbeat(ctx context.Context, args []string) {
	cliCfg := heartbeatCliConfig{
		HeartbeatInterval: 15 * time.Second,
		ExecutiveURL:      executivepkg.DefaultExecutiveURL,
		Dogstatsd:         defaultDogstatsdConfig(),
		WriterName:        "heartbeat",
		FamilyName:        "ctlstore",
		TableName:         "heartbeats",
	}
	loadConfig(&cliCfg, "heartbeat", args)
	if cliCfg.Debug {
		enableDebug()
	}
	_, teardown := configureDogstatsd(ctx, dogstatsdOpts{
		config:      cliCfg.Dogstatsd,
		statsPrefix: "heartbeat",
	})
	defer teardown()
	heartbeat, err := heartbeatpkg.HeartbeatFromConfig(heartbeatpkg.HeartbeatConfig{
		HeartbeatInterval: cliCfg.HeartbeatInterval,
		ExecutiveURL:      cliCfg.ExecutiveURL,
		WriterName:        cliCfg.WriterName,
		WriterSecret:      cliCfg.WriterSecret,
		Family:            cliCfg.FamilyName,
		Table:             cliCfg.TableName,
	})
	if err != nil {
		events.Log("Fatal error starting heartbeat: %+v", err)
		errs.IncrDefault(stats.T("op", "startup"))
		return
	}
	defer heartbeat.Close()
	heartbeat.Start(ctx)
}

func executive(ctx context.Context, args []string) {
	cliCfg := executiveCliConfig{
		Bind:                           "",
		CtlDBDSN:                       "",
		HandlerTimeout:                 30 * time.Second,
		Dogstatsd:                      defaultDogstatsdConfig(),
		WriterLimitPeriod:              time.Minute,
		WriterLimit:                    1000,
		WarnTableSize:                  50 * units.MEGABYTE,
		MaxTableSize:                   100 * units.MEGABYTE,
		EnableDestructiveSchemaChanges: false,
	}

	loadConfig(&cliCfg, "executive", args)

	events.Log("running with max/warn: %v %v", cliCfg.MaxTableSize, cliCfg.WarnTableSize)

	if cliCfg.Debug {
		enableDebug()
	}

	shadow := "false"
	if cliCfg.Shadow {
		shadow = "true"
	}

	_, teardown := configureDogstatsd(ctx, dogstatsdOpts{
		config:      cliCfg.Dogstatsd,
		statsPrefix: "executive",
		defaultTags: []stats.Tag{stats.T("shadow", shadow)},
	})
	defer teardown()

	executive, err := executivepkg.ExecutiveServiceFromConfig(executivepkg.ExecutiveServiceConfig{
		CtlDBDSN:                       cliCfg.CtlDBDSN,
		RequestTimeout:                 cliCfg.HandlerTimeout,
		MaxTableSize:                   cliCfg.MaxTableSize,
		WarnTableSize:                  cliCfg.WarnTableSize,
		WriterLimit:                    cliCfg.WriterLimit,
		WriterLimitPeriod:              cliCfg.WriterLimitPeriod,
		EnableDestructiveSchemaChanges: cliCfg.EnableDestructiveSchemaChanges,
	})
	if err != nil {
		errs.IncrDefault(stats.T("op", "startup"))
		events.Log("Fatal error starting Executive: %{error}+v", err)
		return
	}
	defer executive.Close()

	if err := executive.Start(ctx, cliCfg.Bind); err != nil {
		if errors.Cause(err) != context.Canceled {
			errs.IncrDefault(stats.T("op", "service shutdown"))
		}
		events.Log("executive quit: %v", err)
	}
}

func sidecar(ctx context.Context, args []string) {
	config := sidecarConfig{
		BindAddr:  "0.0.0.0:1331",
		Dogstatsd: defaultDogstatsdConfig(),
	}
	loadConfig(&config, "sidecar", args)
	dd, teardown := configureDogstatsd(ctx, dogstatsdOpts{
		config:            config.Dogstatsd,
		statsPrefix:       "sidecar",
		defaultTagFilters: []string{},
	})
	defer teardown()
	if dd != nil {
		ctlstore.Initialize(ctx, "ctlstore-sidecar", dd)
	}
	sidecar, err := newSidecar(config)
	if err != nil {
		events.Log("Fatal error starting sidecar: %{error}+v", err)
		errs.IncrDefault(stats.T("op", "startup"))
		return
	}
	sidecar.Start(ctx)
}

func reflector(ctx context.Context, args []string) {
	cliCfg := defaultReflectorCLIConfig(false)
	loadConfig(&cliCfg, "reflector", args)
	if cliCfg.Debug {
		enableDebug()
	}

	var promHandler *prometheus.Handler
	if len(cliCfg.MetricsBind) > 0 {
		promHandler = &prometheus.Handler{}

		http.Handle("/metrics", promHandler)

		go func() {
			events.Log("Serving Prometheus metrics on %s", cliCfg.MetricsBind)
			err := http.ListenAndServe(cliCfg.MetricsBind, nil)
			if err != nil {
				events.Log("Failed to served Prometheus metrics: %s", err)
			}
		}()
	}
	_, teardown := configureDogstatsd(ctx, dogstatsdOpts{
		config:            cliCfg.Dogstatsd,
		statsPrefix:       "reflector",
		prometheusHandler: promHandler,
	})
	defer teardown()
	reflector, err := newReflector(cliCfg, false, 0)
	if err != nil {
		events.Log("Fatal error starting Reflector: %{error}+v", err)
		errs.IncrDefault(stats.T("op", "startup"))
		return
	}
	reflector.Start(ctx)
}

func multiReflector(ctx context.Context, args []string) {
	cliCfg := defaultReflectorCLIConfig(false)
	loadConfig(&cliCfg, "reflector", args)

	if cliCfg.Debug {
		enableDebug()
	}

	if len(cliCfg.MultiReflector.LDBPaths) <= 1 {
		panic("multi-reflector mode requires at least 2 ldb paths")
	}

	var promHandler *prometheus.Handler
	if len(cliCfg.MetricsBind) > 0 {
		promHandler = &prometheus.Handler{}

		http.Handle("/metrics", promHandler)

		go func() {
			events.Log("Serving Prometheus metrics on %s", cliCfg.MetricsBind)
			err := http.ListenAndServe(cliCfg.MetricsBind, nil)
			if err != nil {
				events.Log("Failed to served Prometheus metrics: %s", err)
			}
		}()
	}
	_, teardown := configureDogstatsd(ctx, dogstatsdOpts{
		config:            cliCfg.Dogstatsd,
		statsPrefix:       "reflector",
		prometheusHandler: promHandler,
	})
	defer teardown()

	reflectors := make([]*reflectorpkg.Reflector, len(cliCfg.MultiReflector.LDBPaths))
	var wg sync.WaitGroup
	errChan := make(chan error, len(cliCfg.MultiReflector.LDBPaths))
	wg.Add(len(cliCfg.MultiReflector.LDBPaths))
	for i, ldbPath := range cliCfg.MultiReflector.LDBPaths {
		p := ldbPath
		x := cliCfg
		x.LDBPath = p
		if i > 0 {
			events.Log("changelog only created for 1st ldb path: %{path}, skipping #%{num}d", cliCfg.MultiReflector.LDBPaths[0], i+1)
			x.ChangelogPath = ""
			x.ChangelogSize = 0

		}
		go func(x reflectorCliConfig, idx int) {
			defer wg.Done()
			r, err := newReflector(x, false, idx)
			if err != nil {
				events.Log("Fatal error starting Reflector: %{error}+v", err)
				errs.IncrDefault(stats.T("op", "startup"), stats.T("path", p))
				errChan <- err
				return
			}
			reflectors[idx] = r
		}(x, i)
	}

	wg.Wait()

	select {
	case <-errChan:
		return
	default:
	}

	grp, grpCtx := errgroup.WithContext(ctx)
	for _, reflector := range reflectors {
		r := reflector
		grp.Go(func() error {
			return r.Start(grpCtx)
		})
	}

	err := grp.Wait()
	if err != nil {
		events.Log("reflectors ended in error %{error}v", err)
		errs.Incr("multi.shutdown", stats.T("err", reflect.ValueOf(err).Type().String()))
		return
	}
}

func defaultReflectorCLIConfig(isSupervisor bool) reflectorCliConfig {
	config := reflectorCliConfig{
		LDBPath:                "",
		ChangelogPath:          "",
		ChangelogSize:          1 * 1024 * 1024,
		UpstreamDriver:         "",
		UpstreamDSN:            "",
		UpstreamLedgerTable:    "ctlstore_dml_ledger",
		UpstreamShardingFamily: "flagon2,cob",
		UpstreamShardingTable:  "flagon2___flags,cob___kvs",
		BootstrapURL:           "",
		PollInterval:           1 * time.Second,
		PollJitterCoefficient:  0.25,
		QueryBlockSize:         100,
		Dogstatsd:              defaultDogstatsdConfig(),
		PollTimeout:            5 * time.Second,
		LedgerHealth: ledgerHealthConfig{
			Disable:                 false,
			MaxHealthyLatency:       time.Minute,
			AttributeName:           "ctlstore-status",
			HealthyAttributeValue:   "healthy",
			UnhealthyAttributeValue: "unhealthy",
			PollInterval:            10 * time.Second,
			AWSRegion:               os.Getenv("AWS_REGION"),
		},
		// disabled by default
		WALPollInterval: 0,
		// 8 MB, double what a "healthy" WAL file should be https://www.sqlite.org/compile.html#default_wal_autocheckpoint
		WALCheckpointThresholdSize: 8 * 1024 * 1024,
		WALCheckpointType:          ldbwriter.Passive,
	}
	if isSupervisor {
		// the supervisor runs as an ECS task, so it cannot yet set
		// an instance attribute
		config.LedgerHealth.Disable = true
	}
	return config
}

func newSidecar(config sidecarConfig) (*sidecarpkg.Sidecar, error) {
	var reader *ctlstore.LDBReader
	var err error
	if config.LDBPath == "" {
		reader, err = ctlstore.Reader()
	} else {
		reader, err = ctlstore.ReaderForPath(config.LDBPath)
	}
	if err != nil {
		return nil, err
	}
	return sidecarpkg.New(sidecarpkg.Config{
		BindAddr:    config.BindAddr,
		Reader:      reader,
		MaxRows:     config.MaxRows,
		Application: config.Application,
	})
}

func newReflector(cliCfg reflectorCliConfig, isSupervisor bool, i int) (*reflectorpkg.Reflector, error) {
	if cliCfg.LedgerHealth.Disable {
		events.Log("DEPRECATION NOTICE: use --disable-ecs-behavior instead of --disable to control this ledger monitor behavior")
	}
	id := fmt.Sprintf("%s-%d", path.Base(cliCfg.LDBPath), i)
	l := events.NewLogger(events.DefaultHandler).With(events.Args{{"id", id}})
	l.EnableDebug = cliCfg.Debug
	return reflectorpkg.ReflectorFromConfig(reflectorpkg.ReflectorConfig{
		LDBPath:         cliCfg.LDBPath,
		ChangelogPath:   cliCfg.ChangelogPath,
		ChangelogSize:   cliCfg.ChangelogSize,
		BootstrapURL:    cliCfg.BootstrapURL,
		BootstrapRegion: cliCfg.BootstrapRegion,
		IsSupervisor:    isSupervisor,
		LedgerHealth: ledger.HealthConfig{
			DisableECSBehavior:      cliCfg.LedgerHealth.Disable || cliCfg.LedgerHealth.DisableECSBehavior,
			MaxHealthyLatency:       cliCfg.LedgerHealth.MaxHealthyLatency,
			AttributeName:           cliCfg.LedgerHealth.AttributeName,
			HealthyAttributeValue:   cliCfg.LedgerHealth.HealthyAttributeValue,
			UnhealthyAttributeValue: cliCfg.LedgerHealth.UnhealthyAttributeValue,
			PollInterval:            cliCfg.LedgerHealth.PollInterval,
			AWSRegion:               cliCfg.LedgerHealth.AWSRegion,
		},
		Upstream: reflectorpkg.UpstreamConfig{
			Driver:                cliCfg.UpstreamDriver,
			DSN:                   cliCfg.UpstreamDSN,
			LedgerTable:           cliCfg.UpstreamLedgerTable,
			ShardingFamily:        cliCfg.UpstreamShardingFamily,
			ShardingTable:         cliCfg.UpstreamShardingTable,
			PollInterval:          cliCfg.PollInterval,
			PollJitterCoefficient: cliCfg.PollJitterCoefficient,
			QueryBlockSize:        cliCfg.QueryBlockSize,
			PollTimeout:           cliCfg.PollTimeout,
		},
		WALPollInterval:            cliCfg.WALPollInterval,
		DoMonitorWAL:               cliCfg.WALPollInterval > 0,
		WALCheckpointThresholdSize: cliCfg.WALCheckpointThresholdSize,
		WALCheckpointType:          cliCfg.WALCheckpointType,
		BusyTimeoutMS:              cliCfg.BusyTimeoutMS,
		ID:                         id,
		Logger:                     l,
	})
}
