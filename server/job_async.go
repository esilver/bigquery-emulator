package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/connection"
	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/internal/metadata"
	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
)

// Async query-job machinery (issue #3).
//
// jobs.insert no longer executes the query before answering: it persists the
// job in state RUNNING, launches a job goroutine, waits a short grace period
// (fast jobs still answer DONE, like the old synchronous handler) and
// returns. getQueryResults long-polls the live job until it completes or
// timeoutMs elapses. The anonymous result table real BigQuery would create
// is registered lazily and only materialized when a client addresses it.
const (
	// insertDoneGrace is how long jobs.insert waits for the job before
	// answering with a non-terminal state. Fast queries finish inside the
	// grace and return DONE, which keeps clients that read the insert
	// response state (and the previous synchronous behaviour) working.
	insertDoneGrace = 50 * time.Millisecond

	// defaultGetQueryResultsTimeout is the long-poll budget applied when the
	// client does not send timeoutMs (the python client's server default).
	defaultGetQueryResultsTimeout = 10 * time.Second

	// maxGetQueryResultsTimeout caps client-supplied timeoutMs well below
	// the HTTP server write timeout.
	maxGetQueryResultsTimeout = 2 * time.Minute

	// completedJobRingSize bounds how many completed jobs stay in the live
	// registry (with their in-memory responses); older ones are served from
	// the metadata store.
	completedJobRingSize = 256
)

func liveJobKey(projectID, jobID string) string {
	return projectID + "/" + jobID
}

// liveJob returns the in-process instance of a query job, or nil when the
// job completed long ago (or was issued by another process lifetime) and its
// persisted row is authoritative.
func (s *Server) liveJob(projectID, jobID string) *metadata.Job {
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()
	return s.liveJobs[liveJobKey(projectID, jobID)]
}

// registerAndPersistJob writes the pending job into the metadata store (and
// the live registry). The project is re-hydrated under seqMu: the
// projects-row update inside AddJob must be based on the current job list,
// not the snapshot hydrated before the lock was taken.
func (s *Server) registerAndPersistJob(ctx context.Context, projectID string, job *metadata.Job) error {
	s.seqMu.Lock()
	defer s.seqMu.Unlock()
	project, err := s.metaRepo.FindProject(ctx, projectID)
	if err != nil {
		return err
	}
	if project == nil {
		return errNotFound(fmt.Sprintf("project %s is not found", projectID))
	}
	conn, err := s.connMgr.Connection(ctx, projectID, "")
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	if err := project.AddJob(ctx, tx.Tx(), job); err != nil {
		return fmt.Errorf("failed to add job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit job: %w", err)
	}
	s.jobsMu.Lock()
	s.liveJobs[liveJobKey(projectID, job.ID)] = job
	s.jobsMu.Unlock()
	return nil
}

// markJobCompleted moves the job into the bounded completed ring, evicting
// the oldest completed job (and its in-memory response) when full.
func (s *Server) markJobCompleted(job *metadata.Job) {
	key := liveJobKey(job.ProjectID, job.ID)
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()
	if _, ok := s.liveJobs[key]; !ok {
		return
	}
	s.completedRing = append(s.completedRing, key)
	if len(s.completedRing) > completedJobRingSize {
		evict := s.completedRing[0]
		s.completedRing = s.completedRing[1:]
		delete(s.liveJobs, evict)
	}
}

// startQueryJob launches the goroutine that executes a query job created by
// jobs.insert.
func (s *Server) startQueryJob(job *metadata.Job) {
	s.jobWG.Add(1)
	go s.runQueryJob(job)
}

func (s *Server) runQueryJob(job *metadata.Job) {
	defer s.jobWG.Done()
	ctx := logger.WithLogger(s.jobsCtx, s.logger)

	content := job.Content()
	queryConfig := content.Configuration.Query
	query := queryConfig.Query
	hasDestinationTable := queryConfig.DestinationTable != nil
	stmtType := statementTypeForQuery(query, nil)
	// Read-only jobs (a SELECT without an explicit destination — the dbt
	// test/probe shape) run without the global write lock and therefore
	// overlap with each other; anything that mutates engine or metadata
	// state serializes on seqMu exactly like it did under the old global
	// request mutex.
	readOnly := stmtType == "SELECT" && !hasDestinationTable

	var (
		response *internaltypes.QueryResponse
		jobErr   error
	)
	startTime := time.Now()

	// Finalization (runs after the engine transaction below is settled and
	// all locks are released): flip the job content to its terminal state,
	// persist it, and only then wake the long-poll waiters.
	defer func() {
		if p := recover(); p != nil {
			jobErr = fmt.Errorf("query job panicked: %v", p)
		}
		endTime := time.Now()
		var totalBytes int64
		if response != nil {
			totalBytes = response.TotalBytes
		}
		queryStats := queryJobStatistics(query, response, totalBytes)
		status := &bigqueryv2.JobStatus{State: "DONE"}
		if jobErr != nil {
			jobProtoErr := jobErrorProto(jobErr)
			status.ErrorResult = jobProtoErr.ErrorProto()
			status.Errors = []*bigqueryv2.ErrorProto{jobProtoErr.ErrorProto()}
		}
		job.UpdateContent(func(content *bigqueryv2.Job) {
			content.Status = status
			content.Statistics = &bigqueryv2.JobStatistics{
				Query:               queryStats,
				CreationTime:        startTime.Unix(),
				StartTime:           startTime.Unix(),
				EndTime:             endTime.Unix(),
				TotalBytesProcessed: totalBytes,
			}
		})
		if err := s.persistJobResult(ctx, job, response, jobErr); err != nil {
			s.logger.Error(fmt.Sprintf("failed to persist result of job %s: %v", job.ID, err))
		}
		job.Complete(response, jobErr)
		s.markJobCompleted(job)
	}()

	// The query may reference the anonymous result table of an earlier job;
	// materialize before any lock is taken (lock order: anonMu -> seqMu).
	if err := s.materializeAnonTablesReferencedIn(ctx, job.ProjectID, query); err != nil {
		jobErr = err
		return
	}

	locked := false
	if !readOnly {
		s.seqMu.Lock()
		locked = true
	}
	defer func() {
		if locked {
			s.seqMu.Unlock()
		}
	}()

	conn, err := s.connMgr.Connection(ctx, job.ProjectID, "")
	if err != nil {
		jobErr = fmt.Errorf("failed to get connection: %w", err)
		return
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		jobErr = fmt.Errorf("failed to start transaction: %w", err)
		return
	}
	defer tx.RollbackIfNotCommitted()
	response, jobErr = s.contentRepo.Query(
		ctx,
		tx,
		job.ProjectID,
		"",
		query,
		queryConfig.QueryParameters,
	)
	if jobErr == nil && hasDestinationTable {
		jobErr = s.insertQueryResultIntoDestinationTable(ctx, tx, job, response)
	}
	if jobErr != nil {
		return
	}
	if err := tx.Commit(); err != nil {
		jobErr = fmt.Errorf("failed to commit job: %w", err)
		return
	}
	if response != nil && response.ChangedCatalog.Changed() {
		if err := syncCatalog(ctx, s, response.ChangedCatalog); err != nil {
			jobErr = err
			return
		}
	}
	if !hasDestinationTable && stmtType == "SELECT" &&
		response != nil && response.Schema != nil && len(response.Schema.Fields) > 0 {
		// Advertise the anonymous destination table real BigQuery would
		// create (clients such as the Ruby one read rows through it), but
		// defer the expensive materialization until a client actually
		// addresses it — the dbt/python path reads rows straight from
		// getQueryResults and never touches the table.
		jobID := job.ID
		job.UpdateContent(func(content *bigqueryv2.Job) {
			content.Configuration.Query.DestinationTable = &bigqueryv2.TableReference{
				ProjectId: job.ProjectID,
				DatasetId: jobID,
				TableId:   jobID,
			}
		})
		s.registerAnonTable(job.ProjectID, jobID, jobID)
	}
}

// persistJobResult writes the terminal state of a job into the metadata
// store. It runs in its own short transaction under seqMu.
func (s *Server) persistJobResult(ctx context.Context, job *metadata.Job, response *internaltypes.QueryResponse, jobErr error) error {
	s.seqMu.Lock()
	defer s.seqMu.Unlock()
	conn, err := s.connMgr.Connection(ctx, job.ProjectID, "")
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	if err := job.SetResult(ctx, tx.Tx(), response, jobErr); err != nil {
		return err
	}
	return tx.Commit()
}

// insertQueryResultIntoDestinationTable handles a query job with an explicit
// destination table (jobs.insert configuration.query.destinationTable). The
// caller holds seqMu; the project is re-hydrated here so the metadata writes
// are based on current state.
func (s *Server) insertQueryResultIntoDestinationTable(ctx context.Context, tx *connection.Tx, job *metadata.Job, response *internaltypes.QueryResponse) error {
	content := job.Content()
	queryConfig := content.Configuration.Query
	tableRef := queryConfig.DestinationTable
	tableDef, err := tableDefFromQueryResponse(tableRef.TableId, response)
	if err != nil {
		return err
	}
	project, err := s.metaRepo.FindProject(ctx, job.ProjectID)
	if err != nil {
		return err
	}
	if project == nil {
		return fmt.Errorf("failed to find project: %s", job.ProjectID)
	}
	destinationDataset := project.Dataset(tableRef.DatasetId)
	if destinationDataset == nil {
		return fmt.Errorf("failed to find destination dataset: %s", tableRef.DatasetId)
	}
	destinationTable := destinationDataset.Table(tableRef.TableId)
	if destinationTable == nil {
		// CreateDisposition controls whether a missing destination table is
		// materialized on the fly. CREATE_NEVER must surface the missing
		// table as a 404 (matching real BigQuery and load-job behaviour);
		// CREATE_IF_NEEDED (the default) and an empty value create it from
		// the query's inferred schema.
		if queryConfig.CreateDisposition == "CREATE_NEVER" {
			return errNotFound(fmt.Sprintf(
				"Not found: Table %s:%s.%s",
				tableRef.ProjectId, tableRef.DatasetId, tableRef.TableId,
			))
		}
		if _, err := createTableMetadata(ctx, tx, s, project, destinationDataset, tableDef.ToBigqueryV2(job.ProjectID, tableRef.DatasetId)); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		if serverErr := s.contentRepo.CreateTable(ctx, tx, tableDef.ToBigqueryV2(job.ProjectID, tableRef.DatasetId)); serverErr != nil {
			return fmt.Errorf("failed to create table: %w", serverErr)
		}
	}
	if err := s.contentRepo.AddTableData(ctx, tx, tableRef.ProjectId, tableRef.DatasetId, tableDef); err != nil {
		return fmt.Errorf("failed to add table data: %w", err)
	}
	return nil
}

// registerAnonTable records that the result of jobID is addressable as the
// anonymous table projectID.datasetID.jobID but has not been materialized.
// Lock order: anonMu may be taken before seqMu, never the other way around.
func (s *Server) registerAnonTable(projectID, datasetID, jobID string) {
	s.anonMu.Lock()
	defer s.anonMu.Unlock()
	s.anonTables[liveJobKey(projectID, datasetID)] = jobID
}

// materializeAnonTable creates the anonymous result dataset/table of a
// finished query job, if one is pending for projectID.datasetID. It is a
// cheap no-op (one mutex + map probe) for everything else.
func (s *Server) materializeAnonTable(ctx context.Context, projectID, datasetID string) error {
	key := liveJobKey(projectID, datasetID)
	s.anonMu.Lock()
	defer s.anonMu.Unlock()
	jobID, ok := s.anonTables[key]
	if !ok {
		return nil
	}
	if err := s.materializeAnonResultTable(ctx, projectID, datasetID, jobID); err != nil {
		return err
	}
	delete(s.anonTables, key)
	return nil
}

// materializeAnonTablesReferencedIn materializes every pending anonymous
// table whose name occurs in the query text, so queries against an earlier
// job's destination table keep working. Callers must not hold seqMu.
func (s *Server) materializeAnonTablesReferencedIn(ctx context.Context, projectID, query string) error {
	s.anonMu.Lock()
	var hits []string
	if len(s.anonTables) > 0 {
		prefix := projectID + "/"
		for key := range s.anonTables {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			datasetID := key[len(prefix):]
			if strings.Contains(query, datasetID) {
				hits = append(hits, datasetID)
			}
		}
	}
	s.anonMu.Unlock()
	for _, datasetID := range hits {
		if err := s.materializeAnonTable(ctx, projectID, datasetID); err != nil {
			return err
		}
	}
	return nil
}

// materializeAnonResultTable does the actual work: it loads the job's
// result (in-memory when the job is live, from the metadata store
// otherwise) and creates the dataset + table the eager path used to create
// during jobs.insert.
func (s *Server) materializeAnonResultTable(ctx context.Context, projectID, datasetID, jobID string) error {
	var response *internaltypes.QueryResponse
	if live := s.liveJob(projectID, jobID); live != nil {
		// The job may still be persisting; Wait is bounded by that small
		// window because the anonymous table is only advertised once the
		// query itself succeeded.
		resp, jobErr := live.Wait(ctx)
		if jobErr != nil {
			return nil
		}
		response = resp
	} else {
		found, err := s.metaRepo.FindJob(ctx, projectID, jobID)
		if err != nil {
			return err
		}
		if found == nil {
			return nil
		}
		resp, jobErr := found.Result()
		if jobErr != nil {
			return nil
		}
		response = resp
	}
	if response == nil || response.Schema == nil || len(response.Schema.Fields) == 0 {
		return nil
	}

	s.seqMu.Lock()
	defer s.seqMu.Unlock()
	project, err := s.metaRepo.FindProject(ctx, projectID)
	if err != nil {
		return err
	}
	if project == nil {
		return nil
	}
	if project.Dataset(datasetID) != nil {
		return nil
	}
	tableID := jobID
	tableDef, err := tableDefFromQueryResponse(tableID, response)
	if err != nil {
		return err
	}
	tableDef.SetupMetadata(projectID, datasetID)
	table := metadata.NewTable(s.metaRepo, projectID, datasetID, tableID, tableDef.Metadata)
	dataset := metadata.NewDataset(
		s.metaRepo,
		projectID,
		datasetID,
		&bigqueryv2.Dataset{
			Id: fmt.Sprintf("%s:%s", projectID, datasetID),
			DatasetReference: &bigqueryv2.DatasetReference{
				ProjectId: projectID,
				DatasetId: datasetID,
			},
		},
		[]*metadata.Table{table},
		nil,
		nil,
	)
	conn, err := s.connMgr.Connection(ctx, projectID, "")
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	if err := project.AddDataset(ctx, tx.Tx(), dataset); err != nil {
		return err
	}
	if err := s.metaRepo.AddTable(ctx, tx.Tx(), table); err != nil {
		return err
	}
	if err := s.contentRepo.CreateTable(ctx, tx, tableDef.ToBigqueryV2(projectID, datasetID)); err != nil {
		return err
	}
	if err := s.contentRepo.AddTableData(ctx, tx, projectID, datasetID, tableDef); err != nil {
		return fmt.Errorf("failed to add table data: %w", err)
	}
	return tx.Commit()
}
