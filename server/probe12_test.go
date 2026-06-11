package server_test

// Lane-local re-creation of the issue-#12 probe suite (NOT committed):
//  - tables.get latency while a long engine-bound statement runs
//  - 40-node proxy: 4 workers x (insert job -> poll -> fetch + tables.get)
//    with distinct trivial statements -> nodes/min and median node time.
// Gate: BQE_PROBE=1.

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func TestIssue12Probes(t *testing.T) {
	if os.Getenv("BQE_PROBE") == "" {
		t.Skip("probe suite; set BQE_PROBE=1")
	}
	ctx := context.Background()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject("test"))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()
	client, err := bigquery.NewClient(ctx, "test",
		option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	mustQuery := func(q string) {
		t.Helper()
		it, err := client.Query(q).Read(ctx)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		var row []bigquery.Value
		if err := it.Next(&row); err != nil && err != iterator.Done {
			t.Fatalf("%s read: %v", q, err)
		}
	}
	mustQuery("CREATE SCHEMA IF NOT EXISTS probe")
	mustQuery("CREATE OR REPLACE TABLE probe.seed AS SELECT id FROM UNNEST([1,2,3,4,5]) AS id")

	// Probe 1: tables.get while a heavy statement runs.
	heavyDone := make(chan struct{})
	go func() {
		defer close(heavyDone)
		it, err := client.Query(
			"SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 4000)) a, UNNEST(GENERATE_ARRAY(1, 4000)) b",
		).Read(ctx)
		if err == nil {
			var row []bigquery.Value
			_ = it.Next(&row)
		}
	}()
	time.Sleep(1500 * time.Millisecond) // let the heavy statement reach the engine
	getStart := time.Now()
	if _, err := client.Dataset("probe").Table("seed").Metadata(ctx); err != nil {
		t.Fatalf("tables.get behind heavy query: %v", err)
	}
	tablesGet := time.Since(getStart)
	<-heavyDone
	t.Logf("PROBE tables.get behind heavy query: %s", tablesGet)

	// Probe 2: 40-node proxy, 4 workers, distinct trivial statements.
	const nodes = 40
	var (
		mu    sync.Mutex
		times []time.Duration
	)
	work := make(chan int)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range work {
				ns := time.Now()
				it, err := client.Query(fmt.Sprintf("SELECT %d AS node_val", n)).Read(ctx)
				if err != nil {
					t.Errorf("node %d: %v", n, err)
					continue
				}
				var row []bigquery.Value
				if err := it.Next(&row); err != nil && err != iterator.Done {
					t.Errorf("node %d read: %v", n, err)
				}
				if _, err := client.Dataset("probe").Table("seed").Metadata(ctx); err != nil {
					t.Errorf("node %d tables.get: %v", n, err)
				}
				mu.Lock()
				times = append(times, time.Since(ns))
				mu.Unlock()
			}
		}()
	}
	for n := 0; n < nodes; n++ {
		work <- n
	}
	close(work)
	wg.Wait()
	wall := time.Since(start)
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	median := times[len(times)/2]
	t.Logf("PROBE 40-node proxy: wall=%s rate=%.0f nodes/min median=%s",
		wall, float64(nodes)/wall.Minutes(), median)
}
