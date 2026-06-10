package server_test

// Regression tests for the dbt-bigquery correctness batch (issues #4 #5 #6 #7):
//
//   #4 query jobs must report the real statementType (and DDL jobs must not
//      carry a destination table),
//   #5 duplicate dataset/table creation must be HTTP 409 reason "duplicate"
//      (and query-path duplicates a job error with reason "duplicate"),
//   #6 schema field type names are case-insensitive,
//   #7 CREATE OR REPLACE TABLE/VIEW must not fail in the post-commit catalog
//      sync, in-process and across a reopen of the same database file.

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
)

func newDBTTestServer(t *testing.T) *server.TestServer {
	t.Helper()
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject("test"))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	t.Cleanup(func() {
		testServer.Close()
		if err := bqServer.Stop(context.Background()); err != nil {
			t.Log(err)
		}
	})
	return testServer
}

func createDataset(t *testing.T, baseURL, datasetID string) (int, map[string]any) {
	t.Helper()
	return httpJSON(t, "POST",
		fmt.Sprintf("%s/bigquery/v2/projects/test/datasets", baseURL),
		fmt.Sprintf(`{"datasetReference":{"projectId":"test","datasetId":"%s"}}`, datasetID),
		nil,
	)
}

func mustCreateDataset(t *testing.T, baseURL, datasetID string) {
	t.Helper()
	if code, res := createDataset(t, baseURL, datasetID); code != http.StatusOK {
		t.Fatalf("failed to create dataset %s: %d %v", datasetID, code, res)
	}
}

// insertQueryJob posts a query job through jobs.insert and returns the job
// resource from the response.
func insertQueryJob(t *testing.T, baseURL, jobID, query string) (int, map[string]any) {
	t.Helper()
	body := fmt.Sprintf(
		`{"jobReference":{"projectId":"test","jobId":"%s"},"configuration":{"query":{"query":%q,"useLegacySql":false}}}`,
		jobID, query,
	)
	return httpJSON(t, "POST", fmt.Sprintf("%s/bigquery/v2/projects/test/jobs", baseURL), body, nil)
}

// awaitQueryJob observes job completion exactly the way real clients do
// since the async jobs.insert change (issue #3): getQueryResults long-polls
// until the job completes (a failed job surfaces as an HTTP error body
// without jobComplete), then jobs.get returns the final job resource with
// its full statistics.
func awaitQueryJob(t *testing.T, baseURL, jobID string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		_, res := httpJSON(t, "GET",
			fmt.Sprintf("%s/bigquery/v2/projects/test/queries/%s?timeoutMs=10000&maxResults=0", baseURL, jobID), "", nil)
		if complete, ok := res["jobComplete"].(bool); !ok || complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s did not complete in time", jobID)
		}
	}
	_, job := httpJSON(t, "GET",
		fmt.Sprintf("%s/bigquery/v2/projects/test/jobs/%s", baseURL, jobID), "", nil)
	return job
}

// jobState extracts status.state from a job resource.
func jobState(res map[string]any) string {
	status := lookupMap(res, "status")
	if status == nil {
		return ""
	}
	state, _ := status["state"].(string)
	return state
}

// syncQuery posts to jobs.query (the synchronous /queries endpoint).
func syncQuery(t *testing.T, baseURL, query string) (int, map[string]any) {
	t.Helper()
	return httpJSON(t, "POST",
		fmt.Sprintf("%s/bigquery/v2/projects/test/queries", baseURL),
		fmt.Sprintf(`{"query":%q,"useLegacySql":false}`, query),
		nil,
	)
}

func lookupMap(res map[string]any, path ...string) map[string]any {
	cur := res
	for _, p := range path {
		next, _ := cur[p].(map[string]any)
		if next == nil {
			return nil
		}
		cur = next
	}
	return cur
}

func errorReason(res map[string]any) string {
	errObj := lookupMap(res, "error")
	if errObj == nil {
		return ""
	}
	errs, _ := errObj["errors"].([]any)
	if len(errs) == 0 {
		return ""
	}
	first, _ := errs[0].(map[string]any)
	reason, _ := first["reason"].(string)
	return reason
}

// Issue #4: jobs.insert must report the real statement kind, and DDL jobs
// must not carry a destination table (dbt-bigquery branches on
// statement_type and only resolves job.destination for SELECT).
func TestQueryJobStatementType(t *testing.T) {
	ts := newDBTTestServer(t)
	mustCreateDataset(t, ts.URL, "d4")

	cases := []struct {
		name            string
		query           string
		wantType        string
		wantDestination bool
		wantDDLOp       string
		wantTargetTable string
		// allowJobError tolerates engine-level execution failures that are
		// unrelated to statement-type reporting (e.g. the MERGE rewriter).
		allowJobError bool
	}{
		{name: "select", query: "SELECT 1 AS x", wantType: "SELECT", wantDestination: true},
		{name: "create table", query: "CREATE TABLE d4.st_plain (x INT64)", wantType: "CREATE_TABLE", wantDDLOp: "CREATE", wantTargetTable: "st_plain"},
		{name: "create table as select", query: "CREATE TABLE d4.st_ctas AS SELECT 1 AS x", wantType: "CREATE_TABLE_AS_SELECT", wantDDLOp: "CREATE", wantTargetTable: "st_ctas"},
		{name: "create view", query: "CREATE VIEW d4.st_v AS SELECT x FROM d4.st_ctas", wantType: "CREATE_VIEW", wantDDLOp: "CREATE", wantTargetTable: "st_v"},
		{name: "create or replace view", query: "CREATE OR REPLACE VIEW d4.st_v AS SELECT x FROM d4.st_ctas", wantType: "CREATE_VIEW", wantDDLOp: "REPLACE", wantTargetTable: "st_v"},
		{name: "insert", query: "INSERT INTO d4.st_plain (x) VALUES (1)", wantType: "INSERT"},
		{name: "update", query: "UPDATE d4.st_plain SET x = 2 WHERE TRUE", wantType: "UPDATE"},
		{name: "delete", query: "DELETE FROM d4.st_plain WHERE x = 2", wantType: "DELETE"},
		{name: "merge", query: "MERGE d4.st_plain T USING (SELECT 3 AS x) S ON T.x = S.x WHEN NOT MATCHED THEN INSERT (x) VALUES (S.x)", wantType: "MERGE", allowJobError: true},
		{name: "alter table", query: "ALTER TABLE d4.st_plain ADD COLUMN y STRING", wantType: "ALTER_TABLE", wantDDLOp: "ALTER"},
		{name: "drop view", query: "DROP VIEW d4.st_v", wantType: "DROP_VIEW", wantDDLOp: "DROP", wantTargetTable: "st_v"},
		{name: "drop table", query: "DROP TABLE d4.st_plain", wantType: "DROP_TABLE", wantDDLOp: "DROP", wantTargetTable: "st_plain"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			jobID := fmt.Sprintf("sttype-%d", i)
			code, res := insertQueryJob(t, ts.URL, jobID, tc.query)
			if code != http.StatusOK {
				t.Fatalf("jobs.insert %q: status %d: %v", tc.query, code, res)
			}
			// jobs.insert is async (issue #3): a slower statement answers
			// with a non-terminal state, and the final statistics
			// (ddlTargetTable in particular) land with completion.
			if jobState(res) != "DONE" {
				res = awaitQueryJob(t, ts.URL, jobID)
			}
			if errResult := lookupMap(res, "status", "errorResult"); errResult != nil && !tc.allowJobError {
				t.Fatalf("jobs.insert %q: job failed: %v", tc.query, errResult)
			}
			stats := lookupMap(res, "statistics", "query")
			if stats == nil {
				t.Fatalf("jobs.insert %q: missing statistics.query: %v", tc.query, res)
			}
			if got, _ := stats["statementType"].(string); got != tc.wantType {
				t.Errorf("statementType = %q, want %q", stats["statementType"], tc.wantType)
			}
			dest := lookupMap(res, "configuration", "query", "destinationTable")
			if tc.wantDestination && dest == nil {
				t.Errorf("SELECT job must carry configuration.query.destinationTable, got none")
			}
			if !tc.wantDestination && dest != nil {
				t.Errorf("non-SELECT job must not carry a destination table, got %v", dest)
			}
			if got, _ := stats["ddlOperationPerformed"].(string); got != tc.wantDDLOp {
				t.Errorf("ddlOperationPerformed = %q, want %q", got, tc.wantDDLOp)
			}
			if tc.wantTargetTable != "" {
				target := lookupMap(stats, "ddlTargetTable")
				if target == nil {
					t.Errorf("DDL job missing ddlTargetTable: %v", stats)
				} else if got, _ := target["tableId"].(string); got != tc.wantTargetTable {
					t.Errorf("ddlTargetTable.tableId = %q, want %q", got, tc.wantTargetTable)
				}
			}
		})
	}
}

// Issue #4/#5 regression: jobs.insert without a jobReference is legal (real
// BigQuery generates one; dbt and curl repros post bare configurations). The
// handler dereferenced job.JobReference.JobId unconditionally, panicked, and
// the recovered panic surfaced as a 500 while the rolled-back transaction
// made the DDL silently vanish — every smoke statementType came back empty
// and the later duplicate-CREATE probe found no table to collide with.
func TestJobsInsertWithoutJobReference(t *testing.T) {
	ts := newDBTTestServer(t)
	mustCreateDataset(t, ts.URL, "noref")

	body := `{"configuration":{"query":{"query":"CREATE TABLE noref.t1 (x INT64)","useLegacySql":false}}}`
	code, res := httpJSON(t, "POST", fmt.Sprintf("%s/bigquery/v2/projects/test/jobs", ts.URL), body, nil)
	if code != http.StatusOK {
		t.Fatalf("jobs.insert without jobReference: status = %d, want 200: %v", code, res)
	}
	jobRef := lookupMap(res, "jobReference")
	if jobRef == nil {
		t.Fatalf("response missing generated jobReference: %v", res)
	}
	jobID, _ := jobRef["jobId"].(string)
	if jobID == "" {
		t.Errorf("generated jobReference.jobId is empty: %v", jobRef)
	}
	if projectID, _ := jobRef["projectId"].(string); projectID != "test" {
		t.Errorf("generated jobReference.projectId = %q, want \"test\"", projectID)
	}
	// jobs.insert is async (issue #3): observe completion through the
	// getQueryResults long poll before asserting on the final job.
	if jobState(res) != "DONE" {
		res = awaitQueryJob(t, ts.URL, jobID)
	}
	if errResult := lookupMap(res, "status", "errorResult"); errResult != nil {
		t.Fatalf("job failed: %v", errResult)
	}
	stats := lookupMap(res, "statistics", "query")
	if got, _ := stats["statementType"].(string); got != "CREATE_TABLE" {
		t.Errorf("statementType = %q, want \"CREATE_TABLE\"", got)
	}
	// The DDL must actually have committed: re-creating the same table must
	// now collide.
	code, res = syncQuery(t, ts.URL, "CREATE TABLE noref.t1 (x INT64)")
	if code != http.StatusConflict {
		t.Errorf("re-create after bare-configuration DDL: status = %d, want 409 (was the DDL rolled back?): %v", code, res)
	}
	if reason := errorReason(res); reason != "duplicate" {
		t.Errorf("reason = %q, want \"duplicate\"", reason)
	}
}

// Issue #5: duplicate dataset/table creation is 409 + reason "duplicate"
// (real BigQuery's "Already Exists" shape), not a retried 500 internalError.
func TestDuplicateCreationConflict(t *testing.T) {
	ts := newDBTTestServer(t)

	t.Run("datasets.insert", func(t *testing.T) {
		if code, res := createDataset(t, ts.URL, "dup"); code != http.StatusOK {
			t.Fatalf("first dataset create: %d %v", code, res)
		}
		code, res := createDataset(t, ts.URL, "dup")
		if code != http.StatusConflict {
			t.Fatalf("duplicate dataset create: status = %d, want 409: %v", code, res)
		}
		if reason := errorReason(res); reason != "duplicate" {
			t.Errorf("reason = %q, want \"duplicate\"", reason)
		}
		msg, _ := lookupMap(res, "error")["message"].(string)
		if !strings.Contains(msg, "Already Exists: Dataset test:dup") {
			t.Errorf("message = %q, want it to contain \"Already Exists: Dataset test:dup\"", msg)
		}
	})

	t.Run("tables.insert", func(t *testing.T) {
		tableBody := `{"tableReference":{"projectId":"test","datasetId":"dup","tableId":"dup_t"},"schema":{"fields":[{"name":"x","type":"INT64"}]}}`
		target := fmt.Sprintf("%s/bigquery/v2/projects/test/datasets/dup/tables", ts.URL)
		if code, res := httpJSON(t, "POST", target, tableBody, nil); code != http.StatusOK {
			t.Fatalf("first table create: %d %v", code, res)
		}
		code, res := httpJSON(t, "POST", target, tableBody, nil)
		if code != http.StatusConflict {
			t.Fatalf("duplicate table create: status = %d, want 409: %v", code, res)
		}
		if reason := errorReason(res); reason != "duplicate" {
			t.Errorf("reason = %q, want \"duplicate\"", reason)
		}
		msg, _ := lookupMap(res, "error")["message"].(string)
		if !strings.Contains(msg, "Already Exists: Table test:dup.dup_t") {
			t.Errorf("message = %q, want it to contain \"Already Exists: Table test:dup.dup_t\"", msg)
		}
	})

	t.Run("jobs.query create table duplicate", func(t *testing.T) {
		code, res := syncQuery(t, ts.URL, "CREATE TABLE dup.dup_t (x INT64)")
		if code != http.StatusConflict {
			t.Fatalf("duplicate CREATE TABLE via jobs.query: status = %d, want 409: %v", code, res)
		}
		if reason := errorReason(res); reason != "duplicate" {
			t.Errorf("reason = %q, want \"duplicate\"", reason)
		}
	})

	t.Run("jobs.insert create table duplicate", func(t *testing.T) {
		code, res := insertQueryJob(t, ts.URL, "dup-job-1", "CREATE TABLE dup.dup_t (x INT64)")
		if code != http.StatusOK {
			t.Fatalf("jobs.insert: status = %d: %v", code, res)
		}
		errResult := lookupMap(res, "status", "errorResult")
		if errResult == nil {
			t.Fatalf("duplicate CREATE TABLE job must carry status.errorResult: %v", res)
		}
		if reason, _ := errResult["reason"].(string); reason != "duplicate" {
			t.Errorf("errorResult.reason = %q, want \"duplicate\": %v", reason, errResult)
		}
	})
}

// Issue #6: schema field type names are case-insensitive in real BigQuery;
// dbt seeds arrive as load jobs with lowercase type names.
func TestSchemaFieldTypeCaseInsensitive(t *testing.T) {
	ts := newDBTTestServer(t)
	mustCreateDataset(t, ts.URL, "d6")
	tablesURL := fmt.Sprintf("%s/bigquery/v2/projects/test/datasets/d6/tables", ts.URL)

	t.Run("lowercase tables.insert", func(t *testing.T) {
		body := `{"tableReference":{"projectId":"test","datasetId":"d6","tableId":"lc"},
			"schema":{"fields":[
				{"name":"n","type":"string"},
				{"name":"a","type":"int64"},
				{"name":"b","type":"boolean"},
				{"name":"f","type":"float64"},
				{"name":"ts","type":"timestamp"},
				{"name":"r","type":"record","fields":[{"name":"nested_v","type":"integer"}]},
				{"name":"tags","type":"string","mode":"repeated"}
			]}}`
		if code, res := httpJSON(t, "POST", tablesURL, body, nil); code != http.StatusOK {
			t.Fatalf("lowercase schema create: %d %v", code, res)
		}
		// Stored metadata is canonicalized to uppercase like real BigQuery.
		code, res := httpJSON(t, "GET", tablesURL+"/lc", "", nil)
		if code != http.StatusOK {
			t.Fatalf("tables.get: %d %v", code, res)
		}
		fields, _ := lookupMap(res, "schema")["fields"].([]any)
		if len(fields) == 0 {
			t.Fatalf("missing schema in tables.get response: %v", res)
		}
		first, _ := fields[0].(map[string]any)
		if typ, _ := first["type"].(string); typ != "STRING" {
			t.Errorf("stored field type = %q, want \"STRING\"", typ)
		}
		// And the table is actually queryable (no TYPE_UNKNOWN DDL).
		if code, res := syncQuery(t, ts.URL, "SELECT n, a, b, f FROM d6.lc"); code != http.StatusOK {
			t.Fatalf("query lowercase-created table: %d %v", code, res)
		}
	})

	t.Run("mixed case", func(t *testing.T) {
		body := `{"tableReference":{"projectId":"test","datasetId":"d6","tableId":"mc"},
			"schema":{"fields":[{"name":"a","type":"InTeGeR"},{"name":"b","type":"Bool"}]}}`
		if code, res := httpJSON(t, "POST", tablesURL, body, nil); code != http.StatusOK {
			t.Fatalf("mixed-case schema create: %d %v", code, res)
		}
		if code, res := syncQuery(t, ts.URL, "SELECT a, b FROM d6.mc"); code != http.StatusOK {
			t.Fatalf("query mixed-case-created table: %d %v", code, res)
		}
	})

	t.Run("unknown type rejected", func(t *testing.T) {
		body := `{"tableReference":{"projectId":"test","datasetId":"d6","tableId":"bad"},
			"schema":{"fields":[{"name":"a","type":"strang"}]}}`
		code, res := httpJSON(t, "POST", tablesURL, body, nil)
		if code != http.StatusBadRequest {
			t.Fatalf("unknown type: status = %d, want 400: %v", code, res)
		}
		if reason := errorReason(res); reason != "invalid" {
			t.Errorf("reason = %q, want \"invalid\"", reason)
		}
	})

	t.Run("dbt seed style load job", func(t *testing.T) {
		metadata := `{"jobReference":{"projectId":"test","jobId":"seed-load-1"},
			"configuration":{"load":{
				"destinationTable":{"projectId":"test","datasetId":"d6","tableId":"seed_t"},
				"schema":{"fields":[{"name":"id","type":"int64"},{"name":"name","type":"string"}]},
				"sourceFormat":"CSV","skipLeadingRows":1}}}`
		boundary := "seedboundary"
		var sb strings.Builder
		sb.WriteString("--" + boundary + "\r\n")
		sb.WriteString("Content-Type: application/json; charset=UTF-8\r\n\r\n")
		sb.WriteString(metadata + "\r\n")
		sb.WriteString("--" + boundary + "\r\n")
		sb.WriteString("Content-Type: text/csv\r\n\r\n")
		sb.WriteString("id,name\n1,alpha\n2,beta\n")
		sb.WriteString("\r\n--" + boundary + "--\r\n")
		code, res := httpJSON(t, "POST",
			fmt.Sprintf("%s/upload/bigquery/v2/projects/test/jobs?uploadType=multipart", ts.URL),
			sb.String(),
			map[string]string{"Content-Type": "multipart/related; boundary=" + boundary},
		)
		if code != http.StatusOK {
			t.Fatalf("multipart load with lowercase schema: %d %v", code, res)
		}
		code, res = syncQuery(t, ts.URL, "SELECT COUNT(*) AS c FROM d6.seed_t")
		if code != http.StatusOK {
			t.Fatalf("query seeded table: %d %v", code, res)
		}
		rows := queryRows(t, res)
		if len(rows) != 1 || rows[0][0] != "2" {
			t.Errorf("seeded row count = %v, want [[2]]", rows)
		}
	})
}

// Issue #7: CREATE OR REPLACE TABLE/VIEW must upsert the metadata catalog
// entry instead of failing with "duplicate: table ...".
func TestCreateOrReplace(t *testing.T) {
	ts := newDBTTestServer(t)
	mustCreateDataset(t, ts.URL, "d7")

	mustQuery := func(t *testing.T, q string) map[string]any {
		t.Helper()
		code, res := syncQuery(t, ts.URL, q)
		if code != http.StatusOK {
			t.Fatalf("%q: status %d: %v", q, code, res)
		}
		return res
	}

	t.Run("view", func(t *testing.T) {
		mustQuery(t, "CREATE VIEW d7.v AS SELECT 1 AS x")
		mustQuery(t, "CREATE OR REPLACE VIEW d7.v AS SELECT 2 AS x")
		res := mustQuery(t, "SELECT x FROM d7.v")
		rows := queryRows(t, res)
		if len(rows) != 1 || rows[0][0] != "2" {
			t.Errorf("replaced view rows = %v, want [[2]]", rows)
		}
	})

	t.Run("table", func(t *testing.T) {
		mustQuery(t, "CREATE TABLE d7.t (x INT64)")
		mustQuery(t, "CREATE OR REPLACE TABLE d7.t (y STRING)")
		// The metadata entry must carry the replaced schema.
		code, res := httpJSON(t, "GET",
			fmt.Sprintf("%s/bigquery/v2/projects/test/datasets/d7/tables/t", ts.URL), "", nil)
		if code != http.StatusOK {
			t.Fatalf("tables.get after replace: %d %v", code, res)
		}
		fields, _ := lookupMap(res, "schema")["fields"].([]any)
		if len(fields) != 1 {
			t.Fatalf("replaced table schema fields = %v, want exactly [y STRING]", fields)
		}
		field, _ := fields[0].(map[string]any)
		if name, _ := field["name"].(string); name != "y" {
			t.Errorf("replaced schema field name = %q, want \"y\"", name)
		}
	})

	t.Run("via jobs.insert", func(t *testing.T) {
		if code, res := insertQueryJob(t, ts.URL, "cor-1", "CREATE TABLE d7.t2 (x INT64)"); code != http.StatusOK {
			t.Fatalf("create: %d %v", code, res)
		}
		code, res := insertQueryJob(t, ts.URL, "cor-2", "CREATE OR REPLACE TABLE d7.t2 (y STRING)")
		if code != http.StatusOK {
			t.Fatalf("create or replace: %d %v", code, res)
		}
		if errResult := lookupMap(res, "status", "errorResult"); errResult != nil {
			t.Fatalf("create or replace job failed: %v", errResult)
		}
	})
}

// Issue #7 (across-restart case): the first CREATE OR REPLACE issued against
// a reopened persistent --database file must succeed, because the metadata
// catalog already holds the object.
func TestCreateOrReplaceAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "catalog.db")
	storage := server.Storage(fmt.Sprintf("file:%s?cache=shared", dbPath))

	srv1, err := server.New(storage)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv1.Load(server.StructSource(types.NewProject("test"))); err != nil {
		t.Fatal(err)
	}
	ts1 := srv1.TestServer()
	mustCreateDataset(t, ts1.URL, "d7r")
	for _, q := range []string{
		"CREATE TABLE d7r.t (x INT64)",
		"CREATE VIEW d7r.v AS SELECT 1 AS x",
	} {
		if code, res := syncQuery(t, ts1.URL, q); code != http.StatusOK {
			t.Fatalf("%q: status %d: %v", q, code, res)
		}
	}
	ts1.Close()
	if err := srv1.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	srv2, err := server.New(storage)
	if err != nil {
		t.Fatal(err)
	}
	ts2 := srv2.TestServer()
	t.Cleanup(func() {
		ts2.Close()
		if err := srv2.Stop(ctx); err != nil {
			t.Log(err)
		}
	})
	for _, q := range []string{
		"CREATE OR REPLACE TABLE d7r.t (y STRING)",
		"CREATE OR REPLACE VIEW d7r.v AS SELECT 2 AS x",
	} {
		if code, res := syncQuery(t, ts2.URL, q); code != http.StatusOK {
			t.Fatalf("after reopen %q: status %d: %v", q, code, res)
		}
	}
}
