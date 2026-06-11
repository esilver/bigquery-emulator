package server

import (
	"compress/gzip"
	"fmt"
	"net/http"
	"runtime"
	"strings"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/internal/metadata"
)

// methodOverrideMiddleware honors the X-HTTP-Method-Override header on POST
// requests. The Java BigQuery client tunnels PATCH (and other verbs) through
// POST with this header by default; without translating it here the request
// would never match the method-specific route registered with the router.
func methodOverrideMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			if override := strings.TrimSpace(r.Header.Get("X-HTTP-Method-Override")); override != "" {
				r.Method = strings.ToUpper(override)
			}
		}
		next.ServeHTTP(w, r)
	})
}

// sequentialAccessMiddleware serializes every state-MUTATING request on the
// server-wide seqMu. Read-only requests do not take it at all (issue #12):
//
//   - GET routes never mutate engine or metadata state (audited: every GET
//     handler only navigates the hydrated project tree or runs a SELECT in a
//     rolled-back transaction), so they must not queue behind a running
//     statement — the three round trips of one dbt query overlap another
//     query's engine work, and metadata polls stay at ms while models build.
//   - the async query-job routes manage their own locking (issue #3):
//     jobs.insert returns immediately while the query runs in a job
//     goroutine (which takes seqMu only around its write sections), and
//     jobs.getQueryResults parks the request server-side until the job
//     completes — holding a global lock while parked would deadlock the
//     server.
//
// Everything else (POST/PATCH/PUT/DELETE, including the media-upload routes
// and jobs.query, which executes arbitrary statements synchronously) still
// serializes on seqMu, exactly like the historical global request mutex.
// The metadata read cache is invalidated before the lock is released so the
// writer's client observes its own write on its next read.
func sequentialAccessMiddleware(s *Server) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if asyncJobRoute(r) || r.Method == http.MethodGet {
				next.ServeHTTP(w, r)
				return
			}
			s.seqMu.Lock()
			defer func() {
				s.metaCache.invalidate()
				s.seqMu.Unlock()
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// asyncJobRoute reports whether the request is one of the three query-job
// routes that bypass the global serialization middleware. Routes are
// registered both bare and under the /bigquery/v2 prefix, so match on the
// template suffix.
func asyncJobRoute(r *http.Request) bool {
	route := mux.CurrentRoute(r)
	if route == nil {
		return false
	}
	template, err := route.GetPathTemplate()
	if err != nil {
		return false
	}
	// Strip the optional API prefix; the media-upload route
	// (/upload/bigquery/v2/...) must NOT match — it stays serialized.
	template = strings.TrimPrefix(template, "/bigquery/v2")
	switch {
	case r.Method == http.MethodPost && template == "/projects/{projectId}/jobs":
		return true // jobs.insert
	case r.Method == http.MethodGet && template == "/projects/{projectId}/jobs/{jobId}":
		return true // jobs.get
	case r.Method == http.MethodGet && template == "/projects/{projectId}/queries/{jobId}":
		return true // jobs.getQueryResults
	}
	return false
}

// jobsListRoute reports whether the request is jobs.list (GET
// /projects/{projectId}/jobs, bare or under the /bigquery/v2 prefix).
func jobsListRoute(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	route := mux.CurrentRoute(r)
	if route == nil {
		return false
	}
	template, err := route.GetPathTemplate()
	if err != nil {
		return false
	}
	return strings.TrimPrefix(template, "/bigquery/v2") == "/projects/{projectId}/jobs"
}

// anonTableMaterializeMiddleware materializes a lazily-deferred anonymous
// query-result table the moment a client addresses its dataset (tables.get,
// tabledata.list, datasets.get, ...). It runs OUTSIDE the serialization
// middleware because materialization takes seqMu itself.
func anonTableMaterializeMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			params := mux.Vars(r)
			projectID, hasProject := projectIDFromParams(params)
			datasetID, hasDataset := datasetIDFromParams(params)
			if hasProject && hasDataset {
				server := serverFromContext(ctx)
				if err := server.materializeAnonTable(ctx, projectID, datasetID); err != nil {
					errorResponse(ctx, w, errJobInternalError(err.Error()))
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

func recoveryMiddleware(s *Server) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					ctx := logger.WithLogger(r.Context(), s.logger)
					errorResponse(ctx, w, errInternalError(fmt.Sprintf("%+v", err)))
					var frame int = 1
					for {
						_, file, line, ok := runtime.Caller(frame)
						if !ok {
							break
						}
						s.logger.Error(fmt.Sprintf("%d: %v:%d", frame, file, line))
						frame++
					}
					return
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func loggerMiddleware(s *Server) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			next.ServeHTTP(w, r.WithContext(logger.WithLogger(ctx, s.logger)))
		})
	}
}

func accessLogMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.Logger(r.Context()).Info(
				fmt.Sprintf("%s %s", r.Method, r.URL.Path),
				zap.String("query", r.URL.RawQuery),
			)
			next.ServeHTTP(w, r)
		})
	}
}

const (
	contentEncoding  = "Content-Encoding"
	encodingTypeGzip = "gzip"
)

func decompressMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get(contentEncoding) != encodingTypeGzip {
				next.ServeHTTP(w, r)
				return
			}
			ctx := r.Context()
			reader, err := gzip.NewReader(r.Body)
			if err != nil {
				errorResponse(ctx, w, errInvalid(fmt.Sprintf("failed to decode gzip content: %s", err)))
				return
			}
			defer reader.Close()
			r.Body = reader
			next.ServeHTTP(w, r)
		})
	}
}

func withServerMiddleware(s *Server) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(
				w,
				r.WithContext(withServer(r.Context(), s)),
			)
		})
	}
}

func projectIDFromParams(params map[string]string) (string, bool) {
	projectID, exists := params["projectId"]
	if exists {
		return projectID, true
	}
	projectID, exists = params["projectsId"]
	return projectID, exists
}

func datasetIDFromParams(params map[string]string) (string, bool) {
	datasetID, exists := params["datasetId"]
	if exists {
		return datasetID, true
	}
	datasetID, exists = params["datasetsId"]
	return datasetID, exists
}

func jobIDFromParams(params map[string]string) (string, bool) {
	jobID, exists := params["jobId"]
	if exists {
		return jobID, true
	}
	jobID, exists = params["jobsId"]
	return jobID, exists
}

func tableIDFromParams(params map[string]string) (string, bool) {
	tableID, exists := params["tableId"]
	if exists {
		return tableID, true
	}
	tableID, exists = params["tablesId"]
	return tableID, exists
}

func modelIDFromParams(params map[string]string) (string, bool) {
	modelID, exists := params["modelId"]
	if exists {
		return modelID, true
	}
	modelID, exists = params["modelsId"]
	return modelID, exists
}

func routineIDFromParams(params map[string]string) (string, bool) {
	routineID, exists := params["routineId"]
	if exists {
		return routineID, true
	}
	routineID, exists = params["routinesId"]
	return routineID, exists
}

func withProjectMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			params := mux.Vars(r)
			projectID, exists := projectIDFromParams(params)
			if exists {
				server := serverFromContext(ctx)
				// Fast path for polling a live (in-process) query job:
				// jobs.get and getQueryResults only need the project ID and
				// the job itself, and hydrating the project from the
				// metadata store would block behind whatever statement the
				// engine is currently executing — which would break the
				// long-poll guarantee that getQueryResults answers within
				// ~timeoutMs.
				if jobID, ok := jobIDFromParams(params); ok && r.Method == http.MethodGet {
					if live := server.liveJob(projectID, jobID); live != nil {
						ctx = withProject(ctx, metadata.NewProject(server.metaRepo, projectID, nil, nil))
						ctx = withJob(ctx, live)
						next.ServeHTTP(w, r.WithContext(ctx))
						return
					}
				}
				// Read-only routes hydrate through the metadata read cache so
				// they never queue behind a running engine statement at the
				// driver level. jobs.list is excluded: the cached project's
				// job list is eventually consistent (job-row writes do not
				// invalidate the cache), and listing is the one read that is
				// ABOUT that list. Mutating routes hydrate fresh state under
				// seqMu, which their handlers' read-modify-write cycles (e.g.
				// AddDataset rewriting the project's dataset list) depend on.
				var (
					project *metadata.Project
					err     error
				)
				// The async query-job routes hydrate through the cache too
				// (issue #14): jobs.insert runs at full client concurrency
				// (dbt threads x retries), and a fresh FindProject per
				// insert is O(total job rows) of engine statements — the
				// single-threaded engine saturates quadratically as a build
				// accumulates jobs, which is what wedged the 620-node
				// bench. The insert path no longer mutates through the
				// hydrated project: registerAndPersistJob re-probes
				// existence and duplicates inside its own transaction under
				// seqMu, and the load/extract branch re-hydrates fresh
				// state after it takes seqMu itself.
				if (r.Method == http.MethodGet && !jobsListRoute(r)) || asyncJobRoute(r) {
					project, err = server.readProject(ctx, projectID)
				} else {
					project, err = server.metaRepo.FindProject(ctx, projectID)
				}
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprintln(w, err)
					return
				}
				if project == nil {
					errorResponse(ctx, w, errNotFound(fmt.Sprintf("project %s is not found", projectID)))
					return
				}
				ctx = withProject(ctx, project)
			}
			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		})
	}
}

func withDatasetMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			params := mux.Vars(r)
			datasetID, exists := datasetIDFromParams(params)
			if exists {
				project := projectFromContext(ctx)
				dataset := project.Dataset(datasetID)
				if dataset == nil {
					errorResponse(ctx, w, errNotFound(fmt.Sprintf("dataset %s is not found", datasetID)))
					return
				}
				ctx = withDataset(ctx, dataset)
			}
			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		})
	}
}

func withJobMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			params := mux.Vars(r)
			jobID, exists := jobIDFromParams(params)
			if exists && !jobInContext(ctx) {
				project := projectFromContext(ctx)
				// Prefer the live in-process instance of an async query
				// job: it carries the completion channel the long-poll
				// parks on (the hydrated copy is a point-in-time snapshot).
				job := serverFromContext(ctx).liveJob(project.ID, jobID)
				if job == nil {
					job = project.Job(jobID)
				}
				if job == nil {
					// The project may have been served from the metadata read
					// cache, whose job list is eventually consistent; check
					// the store before answering 404.
					if found, err := serverFromContext(ctx).metaRepo.FindJob(ctx, project.ID, jobID); err == nil && found != nil {
						job = found
					}
				}
				if job == nil {
					errorResponse(ctx, w, errNotFound(fmt.Sprintf("job %s is not found", jobID)))
					return
				}
				ctx = withJob(ctx, job)
			}
			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		})
	}
}

func withTableMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			params := mux.Vars(r)
			tableID, exists := tableIDFromParams(params)
			if exists {
				dataset := datasetFromContext(ctx)
				table := dataset.Table(tableID)
				if table == nil {
					errorResponse(ctx, w, errNotFound(fmt.Sprintf("table %s is not found", tableID)))
					return
				}
				ctx = withTable(ctx, table)
			}
			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		})
	}
}

func withModelMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			params := mux.Vars(r)
			modelID, exists := modelIDFromParams(params)
			if exists {
				dataset := datasetFromContext(ctx)
				model := dataset.Model(modelID)
				if model == nil {
					errorResponse(ctx, w, errNotFound(fmt.Sprintf("model %s is not found", modelID)))
					return
				}
				ctx = withModel(ctx, model)
			}
			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		})
	}
}

func withRoutineMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			params := mux.Vars(r)
			routineID, exists := routineIDFromParams(params)
			if exists {
				dataset := datasetFromContext(ctx)
				routine := dataset.Routine(routineID)
				if routine == nil {
					errorResponse(ctx, w, errNotFound(fmt.Sprintf("routine %s is not found", routineID)))
					return
				}
				ctx = withRoutine(ctx, routine)
			}
			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		})
	}
}
