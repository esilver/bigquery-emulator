package server_test

// Issue #17: a DATE predicate (BETWEEN DATE literals) on a CSV-loaded table
// in a join+aggregate query failed with "Invalid Input Error: unexpected
// value type: time.Time". The CSV load path writes event_date as an
// engine-native DATE; the BETWEEN lowering routes the scanned cell through
// googlesqlite's value decoder, which had no time.Time case (fixed in
// googlesqlite internal/value decodeNativeTime).

import (
	"net/http"
	"strconv"
	"testing"
)

func TestIssue17DateBetweenOnCSVLoadedJoin(t *testing.T) {
	ts := newDBTTestServer(t)
	base := ts.URL
	mustCreateDataset(t, base, "d17")

	loadCSV := func(tableID, schemaFields, data string) {
		t.Helper()
		jobJSON := `{"configuration":{"load":{"sourceFormat":"CSV","skipLeadingRows":1,` +
			`"schema":{"fields":[` + schemaFields + `]},` +
			`"destinationTable":{"projectId":"test","datasetId":"d17","tableId":"` + tableID + `"}}}}`
		contentType, body := multipartUpload(jobJSON, data)
		code, res := httpJSON(t, http.MethodPost,
			base+"/upload/bigquery/v2/projects/test/jobs?uploadType=multipart",
			body, map[string]string{"Content-Type": contentType})
		if code != http.StatusOK {
			t.Fatalf("load %s: %d %v", tableID, code, res)
		}
	}

	loadCSV("events_100k",
		`{"name":"id","type":"INTEGER"},{"name":"event_date","type":"DATE"},`+
			`{"name":"publisher_id","type":"INTEGER"},{"name":"campaign_id","type":"INTEGER"},`+
			`{"name":"amount","type":"FLOAT"},{"name":"category","type":"STRING"}`,
		"id,event_date,publisher_id,campaign_id,amount,category\n"+
			"1,2026-06-01,10,100,1.5,a\n"+
			"2,2026-06-10,10,100,2.5,a\n"+
			"3,2026-06-25,20,200,4.0,b\n")
	loadCSV("publishers_1k",
		`{"name":"publisher_id","type":"INTEGER"},{"name":"publisher_name","type":"STRING"},`+
			`{"name":"tier","type":"STRING"},{"name":"region","type":"STRING"}`,
		"publisher_id,publisher_name,tier,region\n"+
			"10,pub-a,gold,us\n"+
			"20,pub-b,silver,eu\n")

	// Control: the same join without the DATE predicate (worked before).
	code, res := syncQuery(t, base, "SELECT COUNT(*) AS c FROM d17.events_100k e JOIN d17.publishers_1k p ON e.publisher_id = p.publisher_id")
	if code != http.StatusOK {
		t.Fatalf("control join: %d %v", code, res)
	}
	if rows := queryRows(t, res); len(rows) != 1 || rows[0][0] != "3" {
		t.Fatalf("control join rows = %v, want [[3]]", rows)
	}

	// The failing query from the issue (table paths dataset-qualified).
	code, res = syncQuery(t, base,
		"SELECT p.region, p.tier, COUNT(*) AS row_count, SUM(e.amount) AS revenue, AVG(e.amount) AS avg_amount "+
			"FROM d17.events_100k e JOIN d17.publishers_1k p ON e.publisher_id = p.publisher_id "+
			"WHERE e.event_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-20' "+
			"GROUP BY p.region, p.tier ORDER BY revenue DESC")
	if code != http.StatusOK {
		t.Fatalf("join+BETWEEN query: %d %v", code, res)
	}
	rows := queryRows(t, res)
	if len(rows) != 1 {
		t.Fatalf("aggregate rows = %v, want exactly 1 group", rows)
	}
	row := rows[0]
	if len(row) != 5 || row[0] != "us" || row[1] != "gold" || row[2] != "2" {
		t.Fatalf("group row = %v, want [us gold 2 <revenue=4> <avg=2>]", row)
	}
	rev, err := strconv.ParseFloat(row[3], 64)
	if err != nil || rev != 4.0 {
		t.Fatalf("revenue = %q (%v), want 4", row[3], err)
	}
	avg, err := strconv.ParseFloat(row[4], 64)
	if err != nil || avg != 2.0 {
		t.Fatalf("avg_amount = %q (%v), want 2", row[4], err)
	}
}
