package executive

import (
	"context"
	"database/sql"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/ctlstore/pkg/ctldb"
	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/sqlgen"
	"github.com/segmentio/ctlstore/pkg/units"
	"github.com/segmentio/log"
)

type TestExecutiveService struct {
	Addr   net.Addr
	ctldb  *sql.DB
	tmpDir string
	h      *http.Server
}

func NewTestExecutiveService(bindTo string) (*TestExecutiveService, error) {
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", filepath.Join(tmpDir, "ctldb.db"))
	if err != nil {
		return nil, err
	}

	err = ctldb.InitializeCtlDB(db, sqlgen.SqlDriverToDriverName)
	if err != nil {
		return nil, err
	}

	svc := &TestExecutiveService{
		tmpDir: tmpDir,
		ctldb:  db,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	svc.h = &http.Server{Handler: svc}

	started := sync.WaitGroup{}
	started.Add(2)

	go func() {
		listener, err := net.Listen("tcp", bindTo)
		if err != nil {
			log.EventLog("Error listening: %{error}+v", err)
			started.Done()
			return
		}

		// Allows for getting the port after random port assignment
		svc.Addr = listener.Addr()
		started.Done()

		if err := svc.h.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.EventLog("Error serving: %{error}+v", err)
		}
	}()

	go func() {
		started.Done()

		<-stop
		svc.shutdown()
	}()

	started.Wait()

	return svc, nil
}

func (s *TestExecutiveService) shutdown() {
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.h.Shutdown(sctx); err != nil {
		log.EventLog("Shutdown error: %{error}+v", err)
	}
}

func (s *TestExecutiveService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	// Setup and tear these down every req to limit thread-safety garbage
	cR := r.WithContext(ctx)
	limiter := newDBLimiter(
		s.ctldb,
		"sqlite3", limits.SizeLimits{
			MaxSize:  100 * units.MEGABYTE,
			WarnSize: 50 * units.MEGABYTE,
		},
		time.Second,
		1000,
	)
	exec := &dbExecutive{DB: s.ctldb, Ctx: ctx, limiter: limiter}
	ep := ExecutiveEndpoint{Exec: exec, HealthChecker: exec}
	defer ep.Close()
	ep.Handler().ServeHTTP(w, cR)
}

func (s *TestExecutiveService) Close() error {
	s.shutdown()
	s.ctldb.Close()
	os.RemoveAll(s.tmpDir)
	return nil
}

func (s *TestExecutiveService) ExecutiveInterface() ExecutiveInterface {
	return &dbExecutive{DB: s.ctldb, Ctx: context.Background()}
}
