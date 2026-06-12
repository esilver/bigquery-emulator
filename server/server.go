package server

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/goccy/bigquery-emulator/internal/connection"
	"github.com/goccy/bigquery-emulator/internal/contentdata"
	"github.com/goccy/bigquery-emulator/internal/metadata"
	"github.com/gorilla/mux"
)

type Server struct {
	Handler        http.Handler
	storage        Storage
	db             *sql.DB
	loggerConfig   *zap.Config
	logger         *zap.Logger
	connMgr        *connection.Manager
	metaRepo       *metadata.Repository
	contentRepo    *contentdata.Repository
	fileCleanup    func() error
	storageUnlock  func() error
	httpServer     *http.Server
	grpcServer     *grpc.Server
	listenCallback func(httpAddr, grpcAddr string)

	// seqMu serializes every state-mutating code path (it replaces the old
	// per-request global mutex): mutating handlers take it for their whole
	// request via sequentialAccessMiddleware, and the async query-job
	// goroutines take it around their metadata/engine write sections.
	// Read-only paths never take it (issue #12): GET routes bypass the
	// middleware and hydrate through metaCache, and read-only (SELECT)
	// query jobs overlap freely (issue #3). What seqMu actually protects:
	// DuckDB write transactions from each other (the engine uses optimistic
	// concurrency control, so concurrent write transactions abort), and the
	// read-modify-write cycles on metadata rows (e.g. AddJob rewriting the
	// projects row from a hydration that must not go stale mid-flight).
	seqMu sync.Mutex

	// metaCache serves project hydration for read-only request paths without
	// touching the database; every seqMu write section invalidates it before
	// its write becomes observable. See metacache.go for the full model.
	metaCache metaCache

	// Live query jobs (and a bounded ring of recently completed ones),
	// keyed by projectID/jobID. getQueryResults long-polls the live
	// instance; once a job is evicted its persisted row serves the reads.
	jobsMu        sync.Mutex
	liveJobs      map[string]*metadata.Job
	completedRing []string

	// Anonymous query results that have not been materialized into a real
	// destination table yet, keyed by projectID/datasetID (the dataset is
	// named after the job). Materialization happens lazily, the first time
	// a client addresses the destination table (tables.get, tabledata.list,
	// a query referencing it) — see anonTableMaterializeMiddleware.
	anonMu     sync.Mutex
	anonTables map[string]string // projectID/datasetID -> jobID

	// Async job goroutines: tracked so Close can drain them, canceled on
	// shutdown through jobsCtx.
	jobWG      sync.WaitGroup
	jobsCtx    context.Context
	jobsCancel context.CancelFunc

	// maxStmtDuration is the statement watchdog budget applied to every
	// async query job's engine span (issue #14); see
	// resolveMaxStatementDuration. Zero disables the watchdog.
	maxStmtDuration time.Duration
}

// SetListenCallback registers a function invoked once the HTTP and gRPC
// listeners are bound, with the addresses they are actually listening on
// (useful when a port of 0 was requested). It exists so the CLI can report
// the bound addresses; the library itself writes nothing to stdout.
func (s *Server) SetListenCallback(callback func(httpAddr, grpcAddr string)) {
	s.listenCallback = callback
}

func New(storage Storage) (*Server, error) {
	server := &Server{
		storage:    storage,
		liveJobs:   map[string]*metadata.Job{},
		anonTables: map[string]string{},
	}
	server.jobsCtx, server.jobsCancel = context.WithCancel(context.Background())
	server.maxStmtDuration = resolveMaxStatementDuration()
	if query, ok := tempStorageQuery(storage); ok {
		f, err := os.CreateTemp("", "")
		if err != nil {
			return nil, fmt.Errorf("failed to create temporary file: %w", err)
		}
		// DuckDB (unlike SQLite) refuses to open an existing file that is
		// not already a valid database, so hand it a fresh path: keep the
		// unique name CreateTemp reserved but remove the empty file itself.
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("failed to close temporary file: %w", err)
		}
		if err := os.Remove(f.Name()); err != nil {
			return nil, fmt.Errorf("failed to remove temporary file: %w", err)
		}
		storage = appendStorageQuery(Storage(fmt.Sprintf("file:%s?cache=shared", f.Name())), query)
		server.storage = storage
		server.fileCleanup = func() error {
			err := os.Remove(f.Name())
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
	}
	// Exclusive single-writer lock on the database file (issue #14): the
	// wasm engine has no file locking, so a second emulator on the same
	// file would corrupt it silently. Must be held BEFORE the engine opens
	// the file.
	unlock, err := lockStorage(storage)
	if err != nil {
		return nil, err
	}
	server.storageUnlock = unlock
	db, err := sql.Open("googlesqlite", string(storage))
	if err != nil {
		return nil, err
	}
	server.db = db
	server.loggerConfig = &zap.Config{
		Level:             zap.NewAtomicLevelAt(zap.ErrorLevel),
		Development:       false,
		Encoding:          "console",
		DisableStacktrace: true,
		EncoderConfig:     zap.NewDevelopmentEncoderConfig(),
		OutputPaths:       []string{"stderr"},
		ErrorOutputPaths:  []string{"stderr"},
	}
	if _, err := server.loggerConfig.Build(); err != nil {
		return nil, fmt.Errorf("invalid default logger config: %w", err)
	}
	server.logger = zap.NewNop()
	metaRepo, err := metadata.NewRepository(db)
	if err != nil {
		return nil, err
	}
	server.connMgr = connection.NewManager(db)
	server.metaRepo = metaRepo
	server.contentRepo = contentdata.NewRepository()

	r := mux.NewRouter()
	for _, handler := range handlers {
		r.Handle(handler.Path, handler.Handler).Methods(handler.HTTPMethod)
		r.Handle(fmt.Sprintf("/bigquery/v2%s", handler.Path), handler.Handler).Methods(handler.HTTPMethod)
	}
	r.Handle(discoveryAPIEndpoint, newDiscoveryHandler(server)).Methods("GET")
	r.Handle(newDiscoveryAPIEndpoint, newDiscoveryHandler(server)).Methods("GET")
	r.Handle(uploadAPIEndpoint, &uploadHandler{}).Methods("POST")
	r.Handle(uploadAPIEndpoint, &uploadContentHandler{}).Methods("PUT")
	r.PathPrefix("/").Handler(&defaultHandler{})
	r.Use(recoveryMiddleware(server))
	r.Use(loggerMiddleware(server))
	r.Use(accessLogMiddleware())
	r.Use(decompressMiddleware())
	r.Use(withServerMiddleware(server))
	// Lazy anonymous-result materialization must run before the
	// serialization middleware: it takes seqMu itself.
	r.Use(anonTableMaterializeMiddleware())
	r.Use(sequentialAccessMiddleware(server))
	r.Use(withProjectMiddleware())
	r.Use(withDatasetMiddleware())
	r.Use(withJobMiddleware())
	r.Use(withTableMiddleware())
	r.Use(withModelMiddleware())
	r.Use(withRoutineMiddleware())
	// The method-override wrapper runs before mux matches a route, so a
	// tunneled PATCH/PUT/DELETE reaches the correct method-specific handler.
	server.Handler = methodOverrideMiddleware(r)
	return server, nil
}

func (s *Server) Close() error {
	defer func() {
		if s.fileCleanup != nil {
			if err := s.fileCleanup(); err != nil {
				log.Printf("failed to cleanup file: %s", err.Error())
			}
		}
	}()
	// Drain the async job goroutines before the database goes away
	// underneath them. Let in-flight queries finish first (canceling them
	// force-closes their driver connections mid-statement, which surfaces
	// as spurious close errors — the old synchronous handler likewise
	// finished its query before the server could be closed); cancellation
	// is the escape hatch for a hung query.
	drained := make(chan struct{})
	go func() {
		s.jobWG.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(30 * time.Second):
		s.jobsCancel()
		<-drained
	}
	s.jobsCancel()
	if err := s.db.Close(); err != nil {
		log.Printf("failed to close database: %s", err.Error())
		return err
	}
	if s.storageUnlock != nil {
		if err := s.storageUnlock(); err != nil {
			log.Printf("failed to release storage lock: %s", err.Error())
		}
	}
	return nil
}

func (s *Server) SetProject(id string) error {
	ctx := context.Background()
	conn, err := s.connMgr.Connection(ctx, id, "")
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	if err := tx.MetadataRepoMode(); err != nil {
		return err
	}
	if err := s.metaRepo.AddProjectIfNotExists(
		ctx,
		tx.Tx(),
		metadata.NewProject(s.metaRepo, id, nil, nil),
	); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.metaCache.invalidate()
	return nil
}

type LogLevel string

const (
	LogLevelUnknown LogLevel = "unknown"
	LogLevelDebug   LogLevel = "debug"
	LogLevelInfo    LogLevel = "info"
	LogLevelWarn    LogLevel = "warn"
	LogLevelError   LogLevel = "error"
	LogLevelFatal   LogLevel = "fatal"
)

func (s *Server) SetLogLevel(level LogLevel) error {
	var atomicLevel zap.AtomicLevel
	switch level {
	case LogLevelDebug:
		atomicLevel = zap.NewAtomicLevelAt(zap.DebugLevel)
	case LogLevelInfo:
		atomicLevel = zap.NewAtomicLevelAt(zap.InfoLevel)
	case LogLevelWarn:
		atomicLevel = zap.NewAtomicLevelAt(zap.WarnLevel)
	case LogLevelError:
		atomicLevel = zap.NewAtomicLevelAt(zap.ErrorLevel)
	case LogLevelFatal:
		atomicLevel = zap.NewAtomicLevelAt(zap.FatalLevel)
	default:
		return fmt.Errorf("unexpected log level %s", level)
	}
	s.loggerConfig.Level = atomicLevel
	logger, err := s.loggerConfig.Build()
	if err != nil {
		return err
	}
	s.logger = logger
	return nil
}

type LogFormat string

const (
	LogFormatConsole LogFormat = "console"
	LogFormatJSON    LogFormat = "json"
)

func (s *Server) SetLogFormat(format LogFormat) error {
	switch format {
	case LogFormatConsole:
		s.loggerConfig.Encoding = "console"
	case LogFormatJSON:
		s.loggerConfig.Encoding = "json"
	default:
		return fmt.Errorf("unexpected log format %s", format)
	}
	logger, err := s.loggerConfig.Build()
	if err != nil {
		return err
	}
	s.logger = logger
	return nil
}

func (s *Server) Load(sources ...Source) error {
	// Sources write metadata outside the request middleware, so the read
	// cache is invalidated here once they are done.
	defer s.metaCache.invalidate()
	for _, source := range sources {
		if err := source(s); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) Serve(ctx context.Context, httpAddr, grpcAddr string) error {
	httpServer := &http.Server{
		Handler:      s.Handler,
		Addr:         httpAddr,
		WriteTimeout: 5 * time.Minute,
		ReadTimeout:  15 * time.Second,
	}
	s.httpServer = httpServer

	grpcServer := grpc.NewServer()
	registerStorageServer(grpcServer, s)
	s.grpcServer = grpcServer

	httpListener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		httpListener.Close()
		return err
	}

	// Hand the actually bound addresses to the caller. With a requested port
	// of 0 the kernel assigns a free port, so httpAddr / grpcAddr are not the
	// real addresses. The library never writes to stdout itself.
	if s.listenCallback != nil {
		s.listenCallback(httpListener.Addr().String(), grpcListener.Addr().String())
	}

	var eg errgroup.Group
	eg.Go(func() error { return grpcServer.Serve(grpcListener) })
	eg.Go(func() error { return httpServer.Serve(httpListener) })
	return eg.Wait()
}

func (s *Server) Stop(ctx context.Context) error {
	defer s.Close()

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}
