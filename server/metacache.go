package server

import (
	"context"
	"sync"

	"github.com/goccy/bigquery-emulator/internal/metadata"
)

// metaCache is a read-through cache of fully hydrated projects serving the
// read-only (GET) request paths without touching the metadata database
// (issue #12).
//
// Why it exists: the engine executes statements under a single driver-level
// mutex (the wasm core is single-threaded), so ANY SQL — including the
// metadata hydration SELECTs behind FindProject — queues behind whatever
// statement is currently executing. Scoping the server's seqMu down stops
// read requests from queueing behind engine work at the SERVER level, but a
// metadata read that goes to the database would still queue behind a
// long-running statement at the DRIVER level. Serving reads from this cache
// lets tables.get, datasets.get/list, jobs.get and friends answer in
// microseconds while the engine is busy.
//
// Coherence model: every metadata-WRITING section invalidates the whole
// cache (generation bump + map reset) BEFORE its write becomes observable to
// the client that issued it — the serialized middleware invalidates before
// releasing seqMu, and the async job goroutine invalidates before waking the
// long-poll waiters in Complete. A read that misses re-hydrates from the
// database and only stores the result if no invalidation happened during
// hydration (generation check), so a hydration that raced a write can never
// stick in the cache.
//
// Job rows are deliberately NOT an invalidation trigger: every query job
// inserts and later updates its job row, and invalidating per job would
// empty the cache on every query — defeating the point under a 4-thread dbt
// build. Job reads are served by the live-job registry (s.liveJobs) first;
// withJobMiddleware falls back to a direct FindJob when a job is in neither
// the registry nor the cached project (e.g. evicted from the completed
// ring). The cached project's job LIST is therefore eventually consistent —
// like jobs.list in real BigQuery — and the jobs.list route bypasses the
// cache entirely.
//
// LOCK ORDER: mu is a LEAF lock. It is taken both while holding seqMu
// (invalidation from serialized write sections) and without it (reads,
// invalidation from the job goroutine after seqMu is released); no other
// lock is ever acquired while holding it.
type metaCache struct {
	mu   sync.RWMutex
	gen  uint64
	byID map[string]*metadata.Project

	// rowCounts caches the COUNT(*) tables.get reports as numRows, keyed by
	// projectID/datasetID/tableID. The count is an ENGINE query, so without
	// the cache a tables.get would queue behind a running statement at the
	// driver level even though its metadata came from the cache. It shares
	// the invalidation generation; the storage write API (the one data-write
	// path that runs outside seqMu) invalidates too.
	rowCounts map[string]int64
}

// invalidate drops every cached project and row count. Callers are the
// metadata- and data-writing sections; see the coherence model above for the
// required ordering.
func (c *metaCache) invalidate() {
	c.mu.Lock()
	c.gen++
	c.byID = nil
	c.rowCounts = nil
	c.mu.Unlock()
}

// lookup returns the cached project (nil on a miss) and the generation a
// subsequent store must present.
func (c *metaCache) lookup(id string) (*metadata.Project, uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.byID[id], c.gen
}

// store caches a hydrated project unless an invalidation happened after the
// caller's lookup (the hydration could then predate the invalidating write).
func (c *metaCache) store(id string, project *metadata.Project, gen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen != gen {
		return
	}
	if c.byID == nil {
		c.byID = map[string]*metadata.Project{}
	}
	c.byID[id] = project
}

// lookupRowCount returns the cached row count for a table (ok=false on a
// miss) and the generation a subsequent storeRowCount must present.
func (c *metaCache) lookupRowCount(key string) (int64, bool, uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	count, ok := c.rowCounts[key]
	return count, ok, c.gen
}

// storeRowCount caches a row count unless an invalidation happened after the
// caller's lookup.
func (c *metaCache) storeRowCount(key string, count int64, gen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen != gen {
		return
	}
	if c.rowCounts == nil {
		c.rowCounts = map[string]int64{}
	}
	c.rowCounts[key] = count
}

// readProject returns a hydrated project for a READ-ONLY request path,
// serving from the cache when possible. Callers must not mutate the returned
// project tree — the write paths hydrate their own instances under seqMu.
func (s *Server) readProject(ctx context.Context, projectID string) (*metadata.Project, error) {
	project, gen := s.metaCache.lookup(projectID)
	if project != nil {
		return project, nil
	}
	project, err := s.metaRepo.FindProject(ctx, projectID)
	if err != nil {
		return nil, err
	}
	if project != nil {
		s.metaCache.store(projectID, project, gen)
	}
	return project, nil
}
