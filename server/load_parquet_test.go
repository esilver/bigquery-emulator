package server_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/google/go-cmp/cmp"
	"github.com/parquet-go/parquet-go"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// parquetLoadRow is the in-process Parquet fixture for TestLoadParquet. The
// temporal columns carry parquet logical types so the reader reports their
// TimeUnit, exercising the micros/day-count -> time.Time conversion in the
// PARQUET load path.
type parquetLoadRow struct {
	ID  int64     `parquet:"ID"`
	Nm  string    `parquet:"Nm"`
	TS  time.Time `parquet:"TS,timestamp(microsecond)"`
	Dt  time.Time `parquet:"Dt,timestamp(microsecond)"`
	Dat int32     `parquet:"Dat,date"`
	Tm  int64     `parquet:"Tm,time(microsecond)"`
}

func TestLoadParquet(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectName, types.NewDataset(datasetName))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ts1 := time.Date(2024, 3, 15, 12, 30, 45, 500000000, time.UTC)
	ts2 := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	// 2024-03-15 is day 19797 since the epoch; 2020-01-02 is day 18263.
	// 12:30:45.5 == 45045500000 micros; 03:04:05 == 11045000000 micros.
	fixture := []parquetLoadRow{
		{ID: 1, Nm: "John", TS: ts1, Dt: ts1, Dat: 19797, Tm: 45045500000},
		{ID: 2, Nm: "Joan", TS: ts2, Dt: ts2, Dat: 18263, Tm: 11045000000},
	}
	var buf bytes.Buffer
	if err := parquet.Write(&buf, fixture); err != nil {
		t.Fatalf("write parquet fixture: %v", err)
	}

	table := client.Dataset(datasetName).Table(tableName)
	source := bigquery.NewReaderSource(bytes.NewReader(buf.Bytes()))
	source.SourceFormat = bigquery.Parquet
	// Parquet schema inference is not implemented, so the load must carry the
	// destination schema explicitly.
	source.Schema = bigquery.Schema{
		{Name: "ID", Type: bigquery.IntegerFieldType},
		{Name: "Nm", Type: bigquery.StringFieldType},
		{Name: "TS", Type: bigquery.TimestampFieldType},
		{Name: "Dt", Type: bigquery.DateTimeFieldType},
		{Name: "Dat", Type: bigquery.DateFieldType},
		{Name: "Tm", Type: bigquery.TimeFieldType},
	}

	job, err := table.LoaderFrom(source).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Err() != nil {
		t.Fatalf("load job failed: %v", status.Err())
	}

	query := client.Query("SELECT ID, Nm, TS, Dt, Dat, Tm FROM dataset1.table_a ORDER BY ID")
	it, err := query.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	type row struct {
		ID  int64
		Nm  string
		TS  time.Time
		Dt  civil.DateTime
		Dat civil.Date
		Tm  civil.Time
	}
	var rows []row
	for {
		var vals []bigquery.Value
		if err := it.Next(&vals); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}
		if len(vals) != 6 {
			t.Fatalf("expected 6 columns, got %d: %v", len(vals), vals)
		}
		rows = append(rows, row{
			ID:  vals[0].(int64),
			Nm:  vals[1].(string),
			TS:  vals[2].(time.Time),
			Dt:  vals[3].(civil.DateTime),
			Dat: vals[4].(civil.Date),
			Tm:  vals[5].(civil.Time),
		})
	}

	want := []row{
		{
			ID:  1,
			Nm:  "John",
			TS:  ts1,
			Dt:  civil.DateTime{Date: civil.Date{Year: 2024, Month: 3, Day: 15}, Time: civil.Time{Hour: 12, Minute: 30, Second: 45, Nanosecond: 500000000}},
			Dat: civil.Date{Year: 2024, Month: 3, Day: 15},
			Tm:  civil.Time{Hour: 12, Minute: 30, Second: 45, Nanosecond: 500000000},
		},
		{
			ID:  2,
			Nm:  "Joan",
			TS:  ts2,
			Dt:  civil.DateTime{Date: civil.Date{Year: 2020, Month: 1, Day: 2}, Time: civil.Time{Hour: 3, Minute: 4, Second: 5}},
			Dat: civil.Date{Year: 2020, Month: 1, Day: 2},
			Tm:  civil.Time{Hour: 3, Minute: 4, Second: 5},
		},
	}
	if diff := cmp.Diff(want, rows); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
}

// TestLoadParquetSchemaless verifies that a Parquet load against a new table
// with no schema fails cleanly (Parquet schema inference is unimplemented)
// rather than nil-dereferencing the table schema. The dereference would
// otherwise be recovered into a 500.
func TestLoadParquetSchemaless(t *testing.T) {
	const (
		projectName = "test"
		datasetName = "dataset1"
		tableName   = "table_a"
	)

	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	project := types.NewProject(projectName, types.NewDataset(datasetName))
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		t.Fatal(err)
	}

	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(
		ctx,
		projectName,
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var buf bytes.Buffer
	if err := parquet.Write(&buf, []parquetLoadRow{{ID: 1, Nm: "John"}}); err != nil {
		t.Fatalf("write parquet fixture: %v", err)
	}

	table := client.Dataset(datasetName).Table(tableName)
	source := bigquery.NewReaderSource(bytes.NewReader(buf.Bytes()))
	source.SourceFormat = bigquery.Parquet
	// No source.Schema: a schemaless Parquet load.

	job, err := table.LoaderFrom(source).Run(ctx)
	if err != nil {
		// A clean rejection at submit time is acceptable too.
		return
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return
	}
	if status.Err() == nil {
		t.Fatalf("expected schemaless Parquet load to fail, got success")
	}
}
