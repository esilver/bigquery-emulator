package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

// Job is a query job. Two populations share this type:
//
//   - Completed jobs hydrated from the metadata store (NewJob): the response
//     and error are whatever was persisted; done is already closed.
//   - Live jobs created by the async jobs.insert path (NewPendingJob): the
//     query runs in a job goroutine, and done is closed by Complete() once
//     the result (or failure) is in. getQueryResults long-polls Wait /
//     WaitForResult on the live instance.
type Job struct {
	ID        string
	ProjectID string
	content   *bigqueryv2.Job
	response  *internaltypes.QueryResponse
	err       error
	mu        sync.RWMutex
	done      chan struct{}
	completed bool
	repo      *Repository
}

func (j *Job) Query() string {
	return j.content.Configuration.Query.Query
}

func (j *Job) QueryParameters() []*bigqueryv2.QueryParameter {
	return j.content.Configuration.Query.QueryParameters
}

func (j *Job) SetResult(ctx context.Context, tx *sql.Tx, response *internaltypes.QueryResponse, err error) error {
	j.mu.Lock()
	j.response = response
	j.err = err
	j.mu.Unlock()
	if err := j.repo.UpdateJob(ctx, tx, j); err != nil {
		return fmt.Errorf("failed to update job: %w", err)
	}
	return nil
}

func (j *Job) Content() *bigqueryv2.Job {
	return j.content
}

// UpdateContent mutates the job's content under the job lock. The async job
// goroutine uses it to flip status/statistics while jobs.get snapshots
// concurrently.
func (j *Job) UpdateContent(update func(content *bigqueryv2.Job)) {
	j.mu.Lock()
	defer j.mu.Unlock()
	update(j.content)
}

// ContentSnapshot returns a copy of the job content that is safe to encode
// concurrently with the job goroutine completing the job. Nested objects the
// goroutine rewrites (Status, Statistics, Configuration.Query) are copied;
// everything else is immutable after creation.
func (j *Job) ContentSnapshot() *bigqueryv2.Job {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if j.content == nil {
		return nil
	}
	content := *j.content
	if j.content.Status != nil {
		status := *j.content.Status
		content.Status = &status
	}
	if j.content.Statistics != nil {
		stats := *j.content.Statistics
		content.Statistics = &stats
	}
	if j.content.Configuration != nil {
		configuration := *j.content.Configuration
		if configuration.Query != nil {
			query := *configuration.Query
			configuration.Query = &query
		}
		content.Configuration = &configuration
	}
	return &content
}

// Complete publishes the job result and wakes every waiter. It is a no-op on
// an already-completed job, so failure paths may call it defensively.
func (j *Job) Complete(response *internaltypes.QueryResponse, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.completed {
		return
	}
	j.response = response
	j.err = err
	j.completed = true
	close(j.done)
}

// Done reports (without blocking) whether the job has completed.
func (j *Job) Done() bool {
	select {
	case <-j.done:
		return true
	default:
		return false
	}
}

// Result returns the published response and error of a completed job.
func (j *Job) Result() (*internaltypes.QueryResponse, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.response, j.err
}

// ReleaseResponse drops the in-memory copy of the result rows once they are
// persisted; later readers reload them from the metadata store (see Wait).
func (j *Job) ReleaseResponse() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.response = nil
}

// Wait blocks until the job completes (or ctx is canceled) and returns its
// result. When the in-memory response was released (or the job object was
// hydrated without one, e.g. YAML-loaded), the persisted result is reloaded
// from the metadata store.
func (j *Job) Wait(ctx context.Context) (*internaltypes.QueryResponse, error) {
	select {
	case <-j.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	response, err := j.Result()
	if err != nil {
		return nil, err
	}
	if response == nil {
		foundJob, ferr := j.repo.FindJob(ctx, j.ProjectID, j.ID)
		if ferr != nil {
			return nil, ferr
		}
		if foundJob != nil {
			return foundJob.response, foundJob.err
		}
	}
	return response, nil
}

// WaitForResult parks the caller until the job completes or the timeout
// elapses, whichever comes first — the getQueryResults long-poll contract.
// completed=false means the timeout won; the response/err are only
// meaningful when completed is true.
func (j *Job) WaitForResult(ctx context.Context, timeout time.Duration) (response *internaltypes.QueryResponse, jobErr error, completed bool, err error) {
	if !j.Done() && timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-j.done:
		case <-timer.C:
			return nil, nil, false, nil
		case <-ctx.Done():
			return nil, nil, false, ctx.Err()
		}
	}
	if !j.Done() {
		return nil, nil, false, nil
	}
	response, jobErr = j.Result()
	if jobErr == nil && response == nil {
		foundJob, ferr := j.repo.FindJob(ctx, j.ProjectID, j.ID)
		if ferr != nil {
			return nil, nil, true, ferr
		}
		if foundJob != nil {
			response, jobErr = foundJob.response, foundJob.err
		}
	}
	return response, jobErr, true, nil
}

func (j *Job) Cancel(ctx context.Context) error {
	// TODO: job needs to be able to rollback
	return nil
}

func (j *Job) Insert(ctx context.Context, tx *sql.Tx) error {
	return j.repo.AddJob(ctx, tx, j)
}

func (j *Job) Delete(ctx context.Context, tx *sql.Tx) error {
	return j.repo.DeleteJob(ctx, tx, j)
}

// NewJob builds an already-completed job (hydration from the metadata store,
// or the synchronous jobs.query path).
func NewJob(repo *Repository, projectID, jobID string, content *bigqueryv2.Job, response *internaltypes.QueryResponse, err error) *Job {
	done := make(chan struct{})
	close(done)
	return &Job{
		ID:        jobID,
		ProjectID: projectID,
		content:   content,
		response:  response,
		err:       err,
		done:      done,
		completed: true,
		repo:      repo,
	}
}

// NewPendingJob builds a job whose query has not finished yet; the async
// jobs.insert path completes it from the job goroutine via Complete.
func NewPendingJob(repo *Repository, projectID, jobID string, content *bigqueryv2.Job) *Job {
	return &Job{
		ID:        jobID,
		ProjectID: projectID,
		content:   content,
		done:      make(chan struct{}),
		repo:      repo,
	}
}
