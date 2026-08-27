package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"wal-demo/internal/store"
	"wal-demo/internal/wal"
)

func testServer(t *testing.T) (*Server, func()) {
	t.Helper()
	dir := t.TempDir()
	log, err := wal.Open(dir, wal.Options{
		SyncPolicy:      wal.SyncBatch,
		BatchSize:       1 << 20,
		FlushInterval:   time.Hour,
		MaxSegmentBytes: 1 << 20,
		MaxRecordBytes:  64,
		MaxPayloadBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.New(log, dir)
	if err != nil {
		t.Fatal(err)
	}
	return New(st, Config{}), func() { _ = log.Close() }
}

func TestHTTPWriteReadMetricsAndFrontend(t *testing.T) {
	server, closeFn := testServer(t)
	defer closeFn()
	handler := server.Handler()

	body := bytes.NewBufferString(`{"key":"name","value":"wal"}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/write", body)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("write status %d: %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/api/v1/kv/name", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("get status %d", response.Code)
	}
	var value map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil || value["value"] != "wal" {
		t.Fatalf("get body %v, %v", value, err)
	}

	for _, endpoint := range []string{"/api/v1/metrics", "/api/v1/health", "/api/v1/segments", "/"} {
		request = httptest.NewRequest(http.MethodGet, endpoint, nil)
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s status %d", endpoint, response.Code)
		}
	}
}

func TestHTTPBatchAndVerify(t *testing.T) {
	server, closeFn := testServer(t)
	defer closeFn()
	handler := server.Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/write/batch", bytes.NewBufferString(`{"count":5}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("batch status %d: %s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/api/v1/verify", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("verify status %d: %s", response.Code, response.Body.String())
	}
}

func TestHTTPItemsPageAndStatus(t *testing.T) {
	srv, cleanup := testServer(t)
	defer cleanup()
	for _, key := range []string{"bravo", "alpha", "charlie"} {
		body := strings.NewReader(`{"key":"` + key + `","value":"value"}`)
		request := httptest.NewRequest(http.MethodPost, "/api/v1/write", body)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		srv.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusCreated {
			t.Fatalf("write %s: %d %s", key, response.Code, response.Body.String())
		}
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/kv?offset=1&limit=1", nil)
	response := httptest.NewRecorder()
	srv.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"key":"bravo"`) || !strings.Contains(response.Body.String(), `"total":3`) {
		t.Fatalf("unexpected page: %d %s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	response = httptest.NewRecorder()
	srv.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"keys":3`) {
		t.Fatalf("unexpected status: %d %s", response.Code, response.Body.String())
	}
}

func TestHTTPSnapshotCatalog(t *testing.T) {
	srv, cleanup := testServer(t)
	defer cleanup()
	write := httptest.NewRequest(http.MethodPost, "/api/v1/write", strings.NewReader(`{"key":"archived","value":"value"}`))
	write.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	srv.Handler().ServeHTTP(response, write)
	if response.Code != http.StatusCreated {
		t.Fatalf("write: %d %s", response.Code, response.Body.String())
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/snapshots/release-1", nil)
	response = httptest.NewRecorder()
	srv.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusCreated || !strings.Contains(response.Body.String(), `"name":"release-1"`) {
		t.Fatalf("create snapshot: %d %s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/api/v1/snapshots", nil)
	response = httptest.NewRecorder()
	srv.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"release-1"`) {
		t.Fatalf("list snapshots: %d %s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodDelete, "/api/v1/snapshots/release-1", nil)
	response = httptest.NewRecorder()
	srv.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("delete snapshot: %d %s", response.Code, response.Body.String())
	}
}
