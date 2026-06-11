package server_test

// Regression tests for issue #16: resumable upload session URLs (and the
// discovery document's root URLs) must be minted from the incoming request's
// Host header, not the server's bind address. Behind any port mapping
// (docker -p, compose, k8s, reverse proxy) the bind address is unreachable
// from the client, so every load_table_from_file failed after the session
// POST succeeded.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// startResumableSession POSTs a resumable upload session request with the
// given Host header (and optional extra headers) and returns the HTTP status,
// the Location header, and the response body.
func startResumableSession(t *testing.T, baseURL, hostHeader, jobID string, headers map[string]string) (int, string, string) {
	t.Helper()
	metadata := fmt.Sprintf(`{"jobReference":{"projectId":"test","jobId":"%s"},
		"configuration":{"load":{
			"destinationTable":{"projectId":"test","datasetId":"d16","tableId":"seed_t"},
			"schema":{"fields":[{"name":"id","type":"int64"},{"name":"name","type":"string"}]},
			"sourceFormat":"CSV","skipLeadingRows":1}}}`, jobID)
	req, err := http.NewRequest(
		"POST",
		baseURL+"/upload/bigquery/v2/projects/test/jobs?uploadType=resumable",
		strings.NewReader(metadata),
	)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if hostHeader != "" {
		req.Host = hostHeader
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header.Get("Location"), string(body)
}

func TestResumableUploadSessionURLUsesRequestHost(t *testing.T) {
	ts := newDBTTestServer(t)
	mustCreateDataset(t, ts.URL, "d16")

	bindAddr := strings.TrimPrefix(ts.URL, "http://")

	// The address the client believes it is talking to. It differs from the
	// httptest bind address, exactly like docker -p 9450:9050.
	const advertisedHost = "127.0.0.1:9450"
	if advertisedHost == bindAddr {
		t.Fatalf("test setup: advertised host accidentally equals bind address %s", bindAddr)
	}

	code, location, body := startResumableSession(t, ts.URL, advertisedHost, "resumable-host-1", nil)
	if code != http.StatusOK {
		t.Fatalf("session POST: status %d: %s", code, body)
	}
	if location == "" {
		t.Fatal("session POST returned no Location header")
	}
	parsed, err := url.Parse(location)
	if err != nil {
		t.Fatalf("session URL %q does not parse: %v", location, err)
	}
	if parsed.Host != advertisedHost {
		t.Fatalf("session URL host = %q, want the request Host %q (bind addr is %q); full URL: %s",
			parsed.Host, advertisedHost, bindAddr, location)
	}
	if parsed.Scheme != "http" {
		t.Errorf("session URL scheme = %q, want \"http\"", parsed.Scheme)
	}
	if !strings.Contains(location, "uploadType=resumable") || !strings.Contains(location, "upload_id=resumable-host-1") {
		t.Errorf("session URL lost its upload parameters: %s", location)
	}

	// Complete the upload USING THE RETURNED URL, through a dialer that maps
	// the advertised address to the real listener — the moral equivalent of
	// the docker port mapping. The PUT must reach the same server and load
	// the rows.
	mappedClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if addr == advertisedHost {
					addr = bindAddr
				}
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		},
	}
	putReq, err := http.NewRequest("PUT", location, strings.NewReader("id,name\n1,alpha\n2,beta\n"))
	if err != nil {
		t.Fatal(err)
	}
	putReq.Header.Set("Content-Type", "text/csv")
	putResp, err := mappedClient.Do(putReq)
	if err != nil {
		t.Fatalf("chunk PUT to session URL %s failed: %v", location, err)
	}
	putBody, _ := io.ReadAll(putResp.Body)
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("chunk PUT: status %d: %s", putResp.StatusCode, putBody)
	}

	code, res := syncQuery(t, ts.URL, "SELECT COUNT(*) AS c FROM d16.seed_t")
	if code != http.StatusOK {
		t.Fatalf("query loaded table: %d %v", code, res)
	}
	rows := queryRows(t, res)
	if len(rows) != 1 || rows[0][0] != "2" {
		t.Errorf("loaded row count = %v, want [[2]]", rows)
	}
}

func TestDiscoveryRootURLUsesRequestHost(t *testing.T) {
	ts := newDBTTestServer(t)

	const advertisedHost = "127.0.0.1:9451"
	for _, endpoint := range []string{"/discovery/v1/apis/bigquery/v2/rest", "/$discovery/rest"} {
		req, err := http.NewRequest("GET", ts.URL+endpoint, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = advertisedHost
		code, res := func() (int, map[string]any) {
			t.Helper()
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			var out map[string]any
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &out); err != nil {
				t.Fatalf("GET %s: cannot decode response: %v", endpoint, err)
			}
			return resp.StatusCode, out
		}()
		if code != http.StatusOK {
			t.Fatalf("GET %s: status %d", endpoint, code)
		}
		want := "http://" + advertisedHost
		for _, key := range []string{"rootUrl", "baseUrl", "mtlsRootUrl"} {
			if got, _ := res[key].(string); got != want {
				t.Errorf("GET %s: %s = %q, want %q", endpoint, key, got, want)
			}
		}
	}
}
