package executive

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"
	ctldbpkg "github.com/segmentio/ctlstore/pkg/ctldb"
	"github.com/segmentio/ctlstore/pkg/errs"
	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/utils"
	"github.com/segmentio/events/v2"
	"github.com/segmentio/stats/v4"
)

type ExecutiveService interface {
	Start(ctx context.Context, bind string) error
	io.Closer
}

type ExecutiveServiceConfig struct {
	CtlDBDSN                       string
	RequestTimeout                 time.Duration
	MaxTableSize                   int64
	WarnTableSize                  int64
	WriterLimitPeriod              time.Duration
	WriterLimit                    int64
	EnableDestructiveSchemaChanges bool
}

type executiveService struct {
	ctldb                          *sql.DB
	limiter                        *dbLimiter
	ctx                            context.Context
	serveTimeout                   time.Duration
	enableDestructiveSchemaChanges bool
}

func ExecutiveServiceFromConfig(config ExecutiveServiceConfig) (ExecutiveService, error) {
	dsn, err := ctldbpkg.SetCtldbDSNParameters(config.CtlDBDSN)
	if err != nil {
		return nil, err
	}
	const dbType = "mysql"
	ctldb, err := sql.Open(dbType, dsn)
	if err != nil {
		return nil, fmt.Errorf("Error when opening MySQL: %v", err)
	}
	defaultTableLimit := limits.SizeLimits{MaxSize: config.MaxTableSize, WarnSize: config.WarnTableSize}
	limiter := newDBLimiter(ctldb, dbType, defaultTableLimit, config.WriterLimitPeriod, config.WriterLimit)
	es := &executiveService{
		ctldb:                          ctldb,
		serveTimeout:                   config.RequestTimeout,
		limiter:                        limiter,
		enableDestructiveSchemaChanges: config.EnableDestructiveSchemaChanges,
	}
	return es, nil
}

func (s *executiveService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(s.ctx, s.serveTimeout)
	defer cancel()

	// Setup and tear these down every req to limit thread-safety garbage
	cR := r.WithContext(ctx)
	exec := &dbExecutive{DB: s.ctldb, Ctx: ctx, limiter: s.limiter}
	ep := ExecutiveEndpoint{
		Exec:                           exec,
		HealthChecker:                  exec,
		EnableDestructiveSchemaChanges: s.enableDestructiveSchemaChanges,
	}
	defer ep.Close()

	events.Debug("Request: %{request}+v", cR)
	ep.Handler().ServeHTTP(w, cR)
}

func (s *executiveService) Start(ctx context.Context, bind string) error {
	s.ctx = ctx

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// tell the limiter to start picking up db changes
	if err := s.limiter.start(ctx); err != nil {
		return errors.Wrap(err, "could not start limiter")
	}

	// perform instrumentation in the background
	go s.instrument(ctx)

	h := &http.Server{Addr: bind, Handler: s}

	go func() {
		events.Log("Listening on %{addr}s...", bind)
		if err := h.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			events.Log("Error listening: %{error}+v", err)
		} else {
			events.Log("Server stopped.")
		}
	}()

	<-stop
	events.Log("Shutting down the server...")
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := h.Shutdown(sctx); err != nil {
		events.Log("Shutdown error: %{error}+v", err)
	}

	return nil
}

func (s *executiveService) instrument(ctx context.Context) {
	utils.CtxFireLoop(ctx, time.Minute, func() {
		// all instrumentation methods will go here
		s.instrumentLedgerRowCount(ctx)
	})
}

func (s *executiveService) instrumentLedgerRowCount(ctx context.Context) {
	row := s.ctldb.QueryRowContext(ctx, "select table_rows from information_schema.tables where table_name=?", dmlLedgerTableName)
	var rowCount int64
	if err := row.Scan(&rowCount); err != nil {
		events.Log("Could not scan ledger row count: %s", err)
		errs.IncrDefault(stats.Tag{Name: "op", Value: "instrument-ledger-row-count"})
		return
	}
	stats.Set("ledger-row-count", rowCount)
}

func (s *executiveService) Close() error {
	return s.ctldb.Close()
}
