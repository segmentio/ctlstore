package reflector

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/ctlstore"
	"github.com/segmentio/ctlstore/pkg/changelog"
	"github.com/segmentio/ctlstore/pkg/ctldb"
	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/segmentio/ctlstore/pkg/ldbwriter"
	"github.com/segmentio/ctlstore/pkg/ledger"
	"github.com/segmentio/ctlstore/pkg/logwriter"
	"github.com/segmentio/ctlstore/pkg/sqlite"
	"github.com/segmentio/errors-go"
	"github.com/segmentio/events/v2"
	_ "github.com/segmentio/events/v2/log" // lets events actually log
	"github.com/segmentio/objconv/json"
	"github.com/segmentio/stats/v4"
)

// Reflector represents a fully materialized reflector, which polls an
// upstream database's ledger table for DML statements and applies them
// to a local SQLite database (LDB).
type Reflector struct {
	shovel        func() (*shovel, error)
	ldb           *sql.DB
	upstreamdb    *sql.DB
	ledgerMonitor *ledger.Monitor
	walMonitor    starter
	stop          chan struct{}
}

// UpstreamConfig specifies how to reach and treat the upstream CtlDB.
type UpstreamConfig struct {
	Driver                string
	DSN                   string
	LedgerTable           string
	QueryBlockSize        int
	PollInterval          time.Duration
	PollTimeout           time.Duration
	PollJitterCoefficient float64
}

// ReflectorConfig is used to configure a Reflector instance that
// is instantiated by ReflectorFromConfig
type ReflectorConfig struct {
	LDBPath          string
	ChangelogPath    string
	ChangelogSize    int
	Upstream         UpstreamConfig
	BootstrapURL     string
	LedgerHealth     ledger.HealthConfig
	IsSupervisor     bool
	LDBWriteCallback ldbwriter.LDBWriteCallback // optional
	BootstrapRegion  string                     // optional
	// How often to poll the WAL stats
	WALPollInterval time.Duration // optional
	// Performs a checkpoint after the WAL file exceeds this size in bytes
	WALCheckpointThresholdSize int // optional
	// What type of checkpoint to perform
	WALCheckpointType ldbwriter.CheckpointType // optional
	DoMonitorWAL      bool                     // optional
	BusyTimeoutMS     int                      // optional
}

type DownloadMetric struct {
	StartTime  string `json:"startTime"`
	Downloaded string `json:"downloaded"`
}

type starter interface {
	Start(ctx context.Context)
}

// Printable returns a "pretty" stringified version of the config
func (c ReflectorConfig) Printable() string {
	if len(c.BootstrapURL) > 200 {
		c.BootstrapURL = c.BootstrapURL[:200] + "...<truncated>"
	}
	c.Upstream.DSN = "<REDACTED>"
	return fmt.Sprintf("%+v", c)
}

// driverNameSequence will be incremented atomically to ensure unique driver names.
// the database/sql package will panic when registering a driver with the same name
// more than once.
var driverNameSequence int64

// ReflectorFromConfig instantiates a Reflector instance using the
// configuration specified by a ReflectorConfig instance
func ReflectorFromConfig(config ReflectorConfig) (*Reflector, error) {
	events.Log("Config: %{config}s", config.Printable())

	if config.BootstrapURL != "" {
		if _, err := os.Stat(config.LDBPath); err != nil {
			switch {
			case os.IsNotExist(err):
				events.Log("LDB File %{file}s doesn't exist, beginning bootstrap...", config.LDBPath)
				err = bootstrapLDB(ldbBootstrapConfig{
					url:                 config.BootstrapURL,
					path:                config.LDBPath,
					restartOnS3NotFound: config.IsSupervisor, // allow supervisor to restart ldb
					region:              config.BootstrapRegion,
				})
				if err != nil {
					return nil, err
				}
			default:
				return nil, err
			}
		} else {
			events.Log("LDB File %{file}s exists, skipping bootstrap.", config.LDBPath)
		}
	}

	// Allows registering multiple watches (only for testing)
	driverName := ldb.LDBDatabaseDriver

	// changeBuffer will accumulate statements in the sqlite pre-update hook and then be
	// queried in the change log writer.
	var changeBuffer sqlite.SQLChangeBuffer

	// use a unique driver name to prevent database/sql panics.
	driverName = fmt.Sprintf("%s_%d", ldb.LDBDatabaseDriver, atomic.AddInt64(&driverNameSequence, 1))
	err := sqlite.RegisterSQLiteWatch(driverName, &changeBuffer)
	if err != nil {
		return nil, err
	}

	// Using the WAL journal mode permits high levels of concurrency. If this is
	// left as the default, writers and readers will get "database is locked"
	// errors if they happen to collide. The reason this is the case is that the
	// standard method makes changes in-place on the database file, and keeps an
	// undo log. Any readers would see the uncommitted changes, which would
	// violate the isolation properties of the database. In WAL mode, the changes
	// themselves are appended to the log instead of the database file. After
	// the log grows large enough, its contents are "checkpointed" into the
	// database file in batch.
	var ldbDB *sql.DB
	var openErr error
	if config.BusyTimeoutMS > 0 {
		ldbDB, openErr = sql.Open(driverName, config.LDBPath+fmt.Sprintf("?_journal_mode=wal&_busy_timeout=%d", config.BusyTimeoutMS))
	} else {
		ldbDB, openErr = sql.Open(driverName, config.LDBPath+"?_journal_mode=wal")
	}

	if openErr != nil {
		return nil, fmt.Errorf("Error when opening LDB at '%v': %v", config.LDBPath, openErr)
	}

	dsn := config.Upstream.DSN
	if config.Upstream.Driver == "mysql" {
		dsn, err = ctldb.SetCtldbDSNParameters(dsn)
		if err != nil {
			return nil, err
		}
	}

	upstreamdb, err := sql.Open(config.Upstream.Driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("Error when opening upstream DB (%v): %v", config.Upstream.Driver, err)
	}

	row := upstreamdb.QueryRow("select max(seq) from " + config.Upstream.LedgerTable)
	var maxKnownSeq sql.NullInt64
	err = row.Scan(&maxKnownSeq)
	if err != nil {
		return nil, errors.Wrap(err, "find max seq from ledger")
	}

	events.Log("Max known ledger sequence: %{seq}d", maxKnownSeq)

	err = emitMetricFromFile()
	if err != nil {
		return nil, errors.Wrap(err, "Fail to emit metric from file")
	}

	// TODO: check Upstream fields

	stop := make(chan struct{})

	// This is a function so that initialization can be redone each
	// time the shovel operation does a crash-and-restart loop. A good
	// example of where this is useful is when the ldbWriter crashes
	// due to some kind of invariant violation. In this case, the
	// ldbWriter AND the dmlSource is basically useless, since it'll just
	// keep trying to repeat the same mistake over and over. Doing this
	// as a function allows recovering all the way back to initializing
	// the and fetching the last known good sequence in the LDB.
	shovel := func() (*shovel, error) {
		sqlDBWriter := &ldbwriter.SqlLdbWriter{Db: ldbDB}
		var writer ldbwriter.LDBWriter = sqlDBWriter

		var ldbWriteCallbacks []ldbwriter.LDBWriteCallback

		useChangelog := config.ChangelogPath != "" && config.ChangelogSize > 0
		if useChangelog {
			slw := &logwriter.SizedLogWriter{
				RotateSize: config.ChangelogSize,
				Path:       config.ChangelogPath,
				FileMode:   0644,
			}

			clw := &changelog.ChangelogWriter{WriteLine: slw}
			ldbWriteCallbacks = append(ldbWriteCallbacks, &ldbwriter.ChangelogCallback{
				ChangelogWriter: clw,
			})
			events.Log("Writing changelog to %{path}s", config.ChangelogPath)
		}

		if config.LDBWriteCallback != nil {
			ldbWriteCallbacks = append(ldbWriteCallbacks, config.LDBWriteCallback)
		}
		writer = &ldbwriter.CallbackWriter{
			DB:           ldbDB,
			Delegate:     writer,
			Callbacks:    ldbWriteCallbacks,
			ChangeBuffer: &changeBuffer,
		}

		err = ldb.EnsureLdbInitialized(context.TODO(), ldbDB)
		if err != nil {
			return nil, fmt.Errorf("Error when initializing LDB: %v", err)
		}

		lastSeq, err := ldb.FetchSeqFromLdb(context.TODO(), ldbDB)
		if err != nil {
			return nil, fmt.Errorf("Error when fetching last sequence from LDB: %v", err)
		}

		src := &sqlDmlSource{
			db:              upstreamdb,
			lastSequence:    lastSeq,
			ledgerTableName: config.Upstream.LedgerTable,
			queryBlockSize:  config.Upstream.QueryBlockSize,
		}

		return &shovel{
			writer:            writer,
			closers:           []io.Closer{sqlDBWriter},
			source:            src,
			pollInterval:      config.Upstream.PollInterval,
			pollTimeout:       config.Upstream.PollTimeout,
			jitterCoefficient: config.Upstream.PollJitterCoefficient,
			abortOnSeqSkip:    true,
			maxSeqOnStartup:   maxKnownSeq.Int64,
			stop:              stop,
		}, nil
	}

	ledgerLatencyFunc := ctlstore.NewLDBReaderFromDB(ldbDB).GetLedgerLatency
	ledgerMon, err := ledger.NewLedgerMonitor(config.LedgerHealth, ledgerLatencyFunc)
	if err != nil {
		return nil, errors.Wrap(err, "build ledger latency monitor")
	}

	var walMon starter

	if config.DoMonitorWAL && config.WALPollInterval > 0 {
		w := &ldbwriter.SqlLdbWriter{Db: ldbDB}
		cper := func() (*ldbwriter.PragmaWALResult, error) {
			return w.Checkpoint(config.WALCheckpointType)
		}
		walMon = NewMonitor(MonitorConfig{
			PollInterval:               config.WALPollInterval,
			Path:                       config.LDBPath + "-wal",
			WALCheckpointThresholdSize: int64(config.WALCheckpointThresholdSize),
		}, cper)
	} else {
		walMon = &noopStarter{}
	}

	return &Reflector{
		shovel:        shovel,
		ldb:           ldbDB,
		upstreamdb:    upstreamdb,
		ledgerMonitor: ledgerMon,
		stop:          stop,
		walMonitor:    walMon,
	}, nil
}

func emitMetricFromFile() error {
	name := "/var/spool/ctlstore/metrics.json"
	metricsFile, err := os.Open(name)
	defer func() {
		err = os.Remove(name)
	}()
	if err != nil {
		return err
	}

	defer func() {
		err = metricsFile.Close()
	}()
	if err != nil {
		return err
	}

	b, err := ioutil.ReadAll(metricsFile)
	if err != nil {
		return err
	}

	var dm DownloadMetric

	err = json.Unmarshal(b, &dm)
	if err != nil {
		return err
	}

	stats.Observe("init_snapshot_download_time", dm.StartTime, stats.Tag{
		Name:  "downloaded",
		Value: dm.Downloaded,
	})

	return nil
}

func (r *Reflector) Start(ctx context.Context) error {
	events.Log("Starting Reflector.")
	go r.ledgerMonitor.Start(ctx)
	go r.walMonitor.Start(ctx)
	for {
		err := func() error {
			shovel, err := r.shovel()
			if err != nil {
				return errors.Wrap(err, "build shovel")
			}
			defer shovel.Close()
			events.Log("Shoveling...")
			stats.Incr("reflector.shovel_start")
			err = shovel.Start(ctx)
			return errors.Wrap(err, "shovel")
		}()
		switch {
		case errs.IsCanceled(err): // this is normal
		case events.IsTermination(errors.Cause(err)): // this is normal
			events.Log("Reflector received termination signal")
		case err != nil:
			switch {
			case errors.Is("SkippedSequence", err):
				// this is instrumented elsewhere and is not an error that we need
				// to handle normally, so we will skip instrumenting this as a
				// shovel_error for now.
			default:
				errs.Incr("reflector.shovel_error")
			}
			events.Log("Error encountered during shoveling: %{error}+v", err)
		}
		select {
		case <-r.stop:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			// Sleep for a bit here to avoid tight crash looping
		}
	}
}

func (r *Reflector) Stop() {
	close(r.stop)
}

func (r *Reflector) Close() error {
	var err error

	events.Log("Close() reflector")

	err = r.ldb.Close()
	if err != nil {
		return err
	}

	err = r.upstreamdb.Close()
	if err != nil {
		return err
	}

	// CR: use errors.Join here
	return nil
}

// this contains all the info necessary to perform an ldb boostrap
type ldbBootstrapConfig struct {
	url                 string
	path                string
	region              string        // optional
	downloadTo          downloadTo    // for testing
	retryDelay          time.Duration // for testing
	restartOnS3NotFound bool          // whether or not to recreate the ldb if no snapshot exists
}

func bootstrapLDB(cfg ldbBootstrapConfig) error {
	shortURL := cfg.url
	if len(shortURL) > 256 {
		shortURL = shortURL[:256]
	}

	events.Log("Bootstrap: %{url}s (region:%{region}q) to %{path}s", shortURL, cfg.region, cfg.path)

	parsed, err := url.Parse(cfg.url)
	if err != nil {
		return err
	}

	scheme := strings.ToLower(parsed.Scheme)

	var dler downloadTo
	switch {
	case cfg.downloadTo != nil:
		// allow a test to mock the downloader
		dler = cfg.downloadTo
	case scheme == "s3":
		bucket := parsed.Host
		key := parsed.Path
		dler = &S3Downloader{
			Region:              cfg.region,
			Bucket:              bucket,
			Key:                 key,
			StartOverOnNotFound: cfg.restartOnS3NotFound,
		}
	case scheme == "data":
		decoded, err := base64.URLEncoding.DecodeString(parsed.Opaque)
		if err != nil {
			return err
		}
		dler = &memoryDownloader{Content: decoded}
	default:
		return errors.Errorf("unsupported scheme '%s' for bootstrap URL '%s'", scheme, cfg.url)
	}

	// Download to a temp file first to prevent leaving a zero-byte file
	// around, which would trigger the "I already have a file" code paths.

	tmpPath := cfg.path + ".tmp"
	defer os.RemoveAll(tmpPath)

	// make the downloading a function so we can retry it
	downloadSnapshot := func() (int64, error) {
		f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		return dler.DownloadTo(f)
	}

	incrError := func(typ string) {
		errs.Incr("snapshot_download_errors", stats.T("type", typ))
	}
	// try the download in a loop until is succeeds, retries are done, or some other error happens
	maxAttempts := 5
	for maxAttempts > 0 {
		maxAttempts--
		var bytes int64
		bytes, err = downloadSnapshot()
		switch {
		case err == nil:
			// success path
			err = os.Rename(tmpPath, cfg.path)
			if err != nil {
				return err
			}
			events.Log("Bootstrap: Downloaded %{bytes}d bytes", bytes)
			return nil
		case errors.Is(errs.ErrTypeTemporary, err):
			incrError("temporary")
			events.Log("Temporary error trying to download snapshot: %{error}s", err)
			delay := cfg.retryDelay
			if delay == 0 {
				delay = time.Second
			}
			events.Log("Retrying in %{delay}s", delay)
			time.Sleep(delay)
		case errors.Is(errs.ErrTypePermanent, err):
			incrError("permanent")
			events.Log("Could not download snapshot: %{error}s", err)
			events.Log("Starting with a new LDB")
			return nil
		default:
			incrError("generic")
			return err
		}
	}
	return errors.Errorf("download of ldb snapshot failed after max attempts reached: %s", err)
}

type noopStarter struct {
}

func (n *noopStarter) Start(ctx context.Context) {}
