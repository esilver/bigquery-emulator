package server_test

import (
	"bytes"
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/option"
)

// dbtClient spins up an in-process emulator with project "test" and returns a
// connected BigQuery client, mirroring how the dbt-bigquery adapter talks to
// the emulator.
func dbtClient(t *testing.T) (*bigquery.Client, context.Context) {
	t.Helper()
	ctx := context.Background()
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
		_ = bqServer.Stop(ctx)
	})
	client, err := bigquery.NewClient(
		ctx, "test",
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, ctx
}

// runDDL executes a statement that has no result set (DDL/DML) and returns the
// completed job so its statistics can be inspected.
func runDDL(t *testing.T, client *bigquery.Client, ctx context.Context, sql string) *bigquery.JobStatus {
	t.Helper()
	q := client.Query(sql)
	job, err := q.Run(ctx)
	if err != nil {
		t.Fatalf("Run(%q) failed: %v", sql, err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait(%q) failed: %v", sql, err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("job(%q) error: %v", sql, err)
	}
	return status
}

// TestDBTCreateSchemaCreatesDataset covers gap #3: dbt creates its target
// dataset by running "CREATE SCHEMA IF NOT EXISTS". The dataset must actually
// exist afterwards (real BigQuery behaviour), not be a silent no-op.
func TestDBTCreateSchemaCreatesDataset(t *testing.T) {
	client, ctx := dbtClient(t)

	runDDL(t, client, ctx, "CREATE SCHEMA IF NOT EXISTS `test`.`jaffle_shop`")

	md, err := client.Dataset("jaffle_shop").Metadata(ctx)
	if err != nil {
		t.Fatalf("dataset jaffle_shop should exist after CREATE SCHEMA: %v", err)
	}
	if md.FullID != "test:jaffle_shop" {
		t.Fatalf("unexpected dataset FullID: %q, want test:jaffle_shop", md.FullID)
	}

	// IF NOT EXISTS must be idempotent.
	runDDL(t, client, ctx, "CREATE SCHEMA IF NOT EXISTS `test`.`jaffle_shop`")
}

// TestDBTDropSchema covers the DROP SCHEMA side of gap #3: the SQL engine
// rejects DROP SCHEMA outright, so the emulator must service it at the metadata
// layer and report statementType=DROP_SCHEMA.
func TestDBTDropSchema(t *testing.T) {
	client, ctx := dbtClient(t)
	runDDL(t, client, ctx, "CREATE SCHEMA `test`.`to_drop`")
	if _, err := client.Dataset("to_drop").Metadata(ctx); err != nil {
		t.Fatalf("dataset should exist before drop: %v", err)
	}

	status := runDDL(t, client, ctx, "DROP SCHEMA `test`.`to_drop`")
	qs, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok {
		t.Fatalf("expected QueryStatistics, got %T", status.Statistics.Details)
	}
	if qs.StatementType != "DROP_SCHEMA" {
		t.Errorf("DROP SCHEMA statementType = %q, want DROP_SCHEMA", qs.StatementType)
	}

	if _, err := client.Dataset("to_drop").Metadata(ctx); err == nil {
		t.Fatal("dataset should be gone after DROP SCHEMA")
	}

	// DROP SCHEMA IF EXISTS on a missing dataset is a no-op (no error).
	runDDL(t, client, ctx, "DROP SCHEMA IF EXISTS `test`.`to_drop`")
}

// TestDBTCreateTableAsStatementType covers gap #2 for CTAS: the job must report
// statementType=CREATE_TABLE_AS_SELECT and the created table must be queryable.
func TestDBTCreateTableAsStatementType(t *testing.T) {
	client, ctx := dbtClient(t)
	runDDL(t, client, ctx, "CREATE SCHEMA IF NOT EXISTS `test`.`ds`")

	status := runDDL(t, client, ctx,
		"CREATE OR REPLACE TABLE `test`.`ds`.`t` AS SELECT 1 AS id, 'x' AS name")

	qs, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok {
		t.Fatalf("expected QueryStatistics, got %T", status.Statistics.Details)
	}
	if qs.StatementType != "CREATE_TABLE_AS_SELECT" {
		t.Errorf("CTAS statementType = %q, want CREATE_TABLE_AS_SELECT", qs.StatementType)
	}

	// The table must be readable with the expected row.
	it := client.Dataset("ds").Table("t").Read(ctx)
	var row []bigquery.Value
	if err := it.Next(&row); err != nil {
		t.Fatalf("reading CTAS table: %v", err)
	}
	if len(row) != 2 {
		t.Fatalf("CTAS row has %d columns, want 2", len(row))
	}
}

// TestDBTCreateViewStatementType covers gap #2 for views: CREATE VIEW must
// report statementType=CREATE_VIEW and the view must resolve.
func TestDBTCreateViewStatementType(t *testing.T) {
	client, ctx := dbtClient(t)
	runDDL(t, client, ctx, "CREATE SCHEMA IF NOT EXISTS `test`.`ds`")
	runDDL(t, client, ctx, "CREATE OR REPLACE TABLE `test`.`ds`.`base` AS SELECT 1 AS id")

	status := runDDL(t, client, ctx,
		"CREATE OR REPLACE VIEW `test`.`ds`.`v` AS SELECT * FROM `test`.`ds`.`base`")

	qs, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok {
		t.Fatalf("expected QueryStatistics, got %T", status.Statistics.Details)
	}
	if qs.StatementType != "CREATE_VIEW" {
		t.Errorf("CREATE VIEW statementType = %q, want CREATE_VIEW", qs.StatementType)
	}
}

// TestDBTSeedLoadWithUnknownSchema covers gap #1: dbt's seed sends a load job
// whose schema field types are unspecified (the Go client omits an empty
// FieldType, which the emulator historically rejected with "Type not found:
// TYPE_UNKNOWN"). The load must succeed by inferring the column types from the
// data instead of failing.
func TestDBTSeedLoadWithUnknownSchema(t *testing.T) {
	client, ctx := dbtClient(t)
	runDDL(t, client, ctx, "CREATE SCHEMA IF NOT EXISTS `test`.`ds`")

	// Schema with names but blank field types, exactly as dbt seeds emit when
	// no column_types are configured. SourceFormat is intentionally left unset:
	// the Python client used by dbt omits it for CSV (CSV is the load default),
	// so the emulator must treat an empty source format as CSV.
	rs := bigquery.NewReaderSource(bytes.NewBufferString("id,name\n1,alice\n2,bob\n"))
	rs.SkipLeadingRows = 1
	rs.Schema = bigquery.Schema{
		{Name: "id"},   // no Type -> serialized without fieldType
		{Name: "name"}, // no Type
	}
	loader := client.Dataset("ds").Table("seed").LoaderFrom(rs)
	loader.WriteDisposition = bigquery.WriteTruncate
	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatalf("seed load Run: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatalf("seed load Wait: %v", err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("seed load job error: %v", err)
	}

	it := client.Dataset("ds").Table("seed").Read(ctx)
	n := 0
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err != nil {
			break
		}
		n++
	}
	if n != 2 {
		t.Fatalf("seed table has %d rows, want 2", n)
	}
}

// TestDBTSeedLoadLowercaseTypes covers the real shape of gap #1: dbt's seed
// loader sends the column schema with LOWERCASE BigQuery type names (e.g.
// "int64", "string") and no source format. BigQuery type names are
// case-insensitive, so the load must succeed; before the fix the lowercase
// types reached the analyzer as TYPE_UNKNOWN and the CREATE TABLE failed.
func TestDBTSeedLoadLowercaseTypes(t *testing.T) {
	client, ctx := dbtClient(t)
	runDDL(t, client, ctx, "CREATE SCHEMA IF NOT EXISTS `test`.`ds`")

	rs := bigquery.NewReaderSource(bytes.NewBufferString("id,amount,name\n1,9.5,alice\n2,8,bob\n"))
	rs.SkipLeadingRows = 1
	// Lowercase type names, exactly as dbt's _agate_to_schema emits.
	rs.Schema = bigquery.Schema{
		{Name: "id", Type: bigquery.FieldType("int64")},
		{Name: "amount", Type: bigquery.FieldType("float64")},
		{Name: "name", Type: bigquery.FieldType("string")},
	}
	loader := client.Dataset("ds").Table("seedlc").LoaderFrom(rs)
	loader.WriteDisposition = bigquery.WriteTruncate
	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatalf("seed load Run: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatalf("seed load Wait: %v", err)
	}
	if err := status.Err(); err != nil {
		t.Fatalf("seed load job error: %v", err)
	}

	md, err := client.Dataset("ds").Table("seedlc").Metadata(ctx)
	if err != nil {
		t.Fatalf("reading seedlc metadata: %v", err)
	}
	if md.NumRows != 2 {
		t.Fatalf("seedlc has %d rows, want 2", md.NumRows)
	}
	// Types must be normalized to canonical upper-case GoogleSQL spellings.
	got := map[string]bigquery.FieldType{}
	for _, f := range md.Schema {
		got[f.Name] = f.Type
	}
	if got["id"] != "INT64" || got["amount"] != "FLOAT64" || got["name"] != "STRING" {
		t.Fatalf("unexpected normalized types: %v", got)
	}
}
