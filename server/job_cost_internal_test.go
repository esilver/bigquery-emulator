package server

// Deterministic regression gate for the issue-#14 quadratic: persisting one
// job must cost the same whether the metadata store holds 10 jobs or 2000.
// Before the fix, registerAndPersistJob hydrated the WHOLE project — every
// job row including its persisted result payload — and rewrote the
// projects-row jobIDs array, so per-insert cost grew linearly with total
// jobs (Θ(N²) over a build). Under a dbt bench at threads 4 with retry
// storms that collapsed the seqMu queue into a server-wide wedge.

import (
	"context"
	"fmt"
	"testing"
	"time"

	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/metadata"
)

func TestJobPersistCostStaysFlat(t *testing.T) {
	if testing.Short() {
		t.Skip("cost regression, skipped in -short")
	}
	ctx := context.Background()
	s, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.SetProject("test"); err != nil {
		t.Fatal(err)
	}

	newJob := func(i int) *metadata.Job {
		return metadata.NewPendingJob(s.metaRepo, "test", fmt.Sprintf("job_cost_%d", i), &bigqueryv2.Job{
			JobReference: &bigqueryv2.JobReference{ProjectId: "test", JobId: fmt.Sprintf("job_cost_%d", i)},
			Configuration: &bigqueryv2.JobConfiguration{
				Query: &bigqueryv2.JobConfigurationQuery{Query: "SELECT 1"},
			},
		})
	}

	const (
		total  = 1200
		window = 100
	)
	persistWindow := func(start int) time.Duration {
		t.Helper()
		begin := time.Now()
		for i := start; i < start+window; i++ {
			if err := s.registerAndPersistJob(ctx, "test", newJob(i)); err != nil {
				t.Fatalf("insert %d: %v", i, err)
			}
		}
		return time.Since(begin)
	}

	first := persistWindow(0)
	for i := window; i < total-window; i += window {
		persistWindow(i)
	}
	last := persistWindow(total - window)

	t.Logf("first %d inserts: %s; last %d inserts (with %d rows present): %s",
		window, first, window, total-window, last)
	// Flat-cost bound with generous noise headroom. The pre-fix behavior is
	// not noise: at 1100 accumulated jobs each insert re-read and
	// re-unmarshaled every prior row, giving last/first ratios an order of
	// magnitude above this.
	if last > 4*first+200*time.Millisecond {
		t.Fatalf("per-insert cost grew with job count: first window %s, last window %s — the #14 quadratic is back", first, last)
	}
}
