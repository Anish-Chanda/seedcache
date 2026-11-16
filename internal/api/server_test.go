package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anish-chanda/seedcache/internal/replication"
	"github.com/rs/zerolog"
)

type fixture struct{ unavailable bool }

func (f fixture) Status() Status { return Status{QBittorrent: Connection{Connected: !f.unavailable}} }
func (f fixture) Torrents() ([]Torrent, error) {
	if f.unavailable {
		return nil, ErrUnavailable
	}
	return []Torrent{{Hash: "abc", Name: "example", TotalSize: 9007199254740993}}, nil
}

type transferFixture struct {
	record     replication.Record
	listErr    error
	getErr     error
	queueErr   error
	retryErr   error
	summaryErr error
	queueCalls int
	retryCalls int
	created    bool
}

func (f *transferFixture) List(context.Context) ([]replication.Record, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return []replication.Record{f.record}, nil
}
func (f *transferFixture) Get(context.Context, string) (replication.Record, error) {
	return f.record, f.getErr
}
func (f *transferFixture) Queue(_ context.Context, hash string) (replication.Record, error) {
	f.queueCalls++
	if f.queueErr != nil {
		return replication.Record{}, f.queueErr
	}
	if !f.created {
		f.created = true
		f.record = replication.Record{Hash: hash, State: replication.StatePending}
	}
	return f.record, nil
}
func (f *transferFixture) Retry(_ context.Context, hash string) (replication.Record, error) {
	f.retryCalls++
	if f.retryErr != nil {
		return replication.Record{}, f.retryErr
	}
	f.record = replication.Record{Hash: hash, State: replication.StatePending}
	return f.record, nil
}
func (f *transferFixture) Summary(context.Context) (replication.Summary, error) {
	if f.summaryErr != nil {
		return replication.Summary{}, f.summaryErr
	}
	return replication.Summary{Pending: 2, Active: 1, Replicated: 3}, nil
}
func (f fixture) Detail(_ context.Context, hash string) (TorrentDetail, error) {
	if f.unavailable {
		return TorrentDetail{}, ErrUnavailable
	}
	if hash != "abc" {
		return TorrentDetail{}, ErrNotFound
	}
	return TorrentDetail{Torrent: Torrent{Hash: hash}, Files: []File{{Path: "example/file", Size: 123}}}, nil
}

func TestTransferEndpointsAndQueueDeduplication(t *testing.T) {
	transfers := &transferFixture{record: replication.Record{Hash: "abc", State: replication.StateReplicated}}
	handler := New(fixture{}, "test", zerolog.Nop(), transfers)
	for _, tt := range []struct {
		method string
		path   string
		status int
	}{
		{httpMethodGet, "/api/v1/transfers", 200},
		{httpMethodGet, "/api/v1/transfers/abc", 200},
		{httpMethodPost, "/api/v1/torrents/abc/sync", 202},
		{httpMethodPost, "/api/v1/torrents/abc/sync", 202},
		{httpMethodPost, "/api/v1/transfers/abc/retry", 202},
	} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(tt.method, tt.path, nil))
		if w.Code != tt.status {
			t.Fatalf("%s %s: status %d, want %d: %s", tt.method, tt.path, w.Code, tt.status, w.Body.String())
		}
		if !json.Valid(w.Body.Bytes()) {
			t.Fatalf("%s %s returned invalid JSON: %s", tt.method, tt.path, w.Body.String())
		}
	}
	if transfers.queueCalls != 2 || transfers.retryCalls != 1 || !transfers.created {
		t.Fatalf("queue calls = %d, retry calls = %d, created = %t", transfers.queueCalls, transfers.retryCalls, transfers.created)
	}
	var list TransferList
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(httpMethodGet, "/api/v1/transfers", nil))
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil || len(list.Transfers) != 1 {
		t.Fatalf("transfer list = %s, err = %v", w.Body.String(), err)
	}
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(httpMethodGet, "/api/v1/status", nil))
	var status Status
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil || status.Replication.Pending != 2 {
		t.Fatalf("status = %s, err = %v", w.Body.String(), err)
	}
}

func TestTransferEndpointErrors(t *testing.T) {
	for _, tt := range []struct {
		name      string
		err       error
		code      int
		errorCode string
		path      string
	}{
		{"unknown", replication.ErrNotFound, 404, "transfer_not_found", "/api/v1/transfers/abc"},
		{"unknown torrent", replication.ErrNotFound, 404, "torrent_not_found", "/api/v1/torrents/abc/sync"},
		{"conflict", replication.ErrConflict, 409, "transfer_conflict", "/api/v1/transfers/abc/retry"},
		{"queue conflict", replication.ErrConflict, 409, "transfer_conflict", "/api/v1/torrents/abc/sync"},
		{"ineligible", replication.ErrIneligible, 409, "torrent_ineligible", "/api/v1/torrents/abc/sync"},
		{"blocked", &replication.BlockedError{Reason: "unsafe path"}, 409, "transfer_blocked", "/api/v1/torrents/abc/sync"},
		{"unavailable", replication.ErrUnavailable, 503, "replication_unavailable", "/api/v1/transfers"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			transfers := &transferFixture{}
			method := httpMethodGet
			switch {
			case tt.path == "/api/v1/transfers":
				transfers.listErr = tt.err
			case strings.HasSuffix(tt.path, "/retry"):
				method = httpMethodPost
				transfers.retryErr = tt.err
			case tt.path == "/api/v1/transfers/abc":
				transfers.getErr = tt.err
			default:
				method = httpMethodPost
				transfers.queueErr = tt.err
			}
			w := httptest.NewRecorder()
			New(fixture{}, "test", zerolog.Nop(), transfers).ServeHTTP(w, httptest.NewRequest(method, tt.path, nil))
			if w.Code != tt.code {
				t.Fatalf("status = %d, want %d: %s", w.Code, tt.code, w.Body.String())
			}
			var response ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.Error.Code != tt.errorCode || response.Error.Message == "" {
				t.Fatalf("invalid error response %s: %v", w.Body.String(), err)
			}
		})
	}
}

const (
	httpMethodGet  = "GET"
	httpMethodPost = "POST"
)

func TestEndpoints(t *testing.T) {
	for _, tt := range []struct {
		name, method, path string
		unavailable        bool
		code               int
	}{
		{"health", "GET", "/api/v1/health", false, 200},
		{"live while unavailable", "GET", "/api/v1/health", true, 200},
		{"degraded status", "GET", "/api/v1/status", true, 200},
		{"version", "GET", "/api/v1/version", false, 200},
		{"list", "GET", "/api/v1/torrents", false, 200},
		{"detail", "GET", "/api/v1/torrents/abc", false, 200},
		{"missing", "GET", "/api/v1/torrents/missing", false, 404},
		{"unavailable list", "GET", "/api/v1/torrents", true, 503},
		{"unavailable detail", "GET", "/api/v1/torrents/abc", true, 503},
		{"replication service omitted", "GET", "/api/v1/transfers", false, 503},
		{"method", "POST", "/api/v1/torrents", false, 405},
		{"unknown", "GET", "/api/v1/unknown", false, 404},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			New(fixture{tt.unavailable}, "test", zerolog.Nop()).ServeHTTP(w, httptest.NewRequest(tt.method, tt.path, nil))
			if w.Code != tt.code {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			if w.Header().Get("Content-Type") != "application/json" {
				t.Fatal("not JSON")
			}
			if !json.Valid(w.Body.Bytes()) {
				t.Fatal(w.Body.String())
			}
			if tt.code >= 400 {
				var e ErrorResponse
				if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil || e.Error.Code == "" || e.Error.Message == "" {
					t.Fatalf("error: %s", w.Body.String())
				}
			}
			if tt.path == "/api/v1/status" {
				var s Status
				json.Unmarshal(w.Body.Bytes(), &s)
				if s.QBittorrent.Connected {
					t.Fatal("reported connected")
				}
			}
			if tt.name == "list" {
				var list TorrentList
				if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil || len(list.Torrents) != 1 || list.Torrents[0].TotalSize != 9007199254740993 {
					t.Fatal(w.Body.String())
				}
			}
			if tt.name == "detail" {
				var d TorrentDetail
				if err := json.Unmarshal(w.Body.Bytes(), &d); err != nil || len(d.Files) != 1 || d.Files[0].Path != "example/file" {
					t.Fatal(w.Body.String())
				}
			}
		})
	}
}

type lifecycleFixture struct {
	transferFixture
	operation string
	hash      string
	err       error
}

func (f *lifecycleFixture) Evict(_ context.Context, hash string) (replication.Record, error) {
	f.operation, f.hash = "evict", hash
	return f.record, f.err
}
func (f *lifecycleFixture) Restore(_ context.Context, hash string) (replication.Record, error) {
	f.operation, f.hash = "restore", hash
	return f.record, f.err
}
func (f *lifecycleFixture) EvictionPlan(context.Context) (replication.EvictionPlan, error) {
	f.operation = "plan"
	return replication.EvictionPlan{}, f.err
}

func TestCacheLifecycleEndpoints(t *testing.T) {
	for _, test := range []struct {
		method, path, operation string
		status                  int
	}{
		{"GET", "/api/v1/evictions/plan", "plan", 200},
		{"POST", "/api/v1/torrents/abc/evict", "evict", 202},
		{"POST", "/api/v1/torrents/abc/restore", "restore", 202},
		{"GET", "/api/v1/torrents/abc/evict", "", 405},
		{"POST", "/api/v1/evictions/plan", "", 405},
	} {
		t.Run(test.method+test.path, func(t *testing.T) {
			service := &lifecycleFixture{transferFixture: transferFixture{record: replication.Record{Hash: "abc", CacheState: "evicted"}}}
			handler := New(fixture{}, "test", zerolog.Nop(), service)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(test.method, test.path, nil))
			if w.Code != test.status || service.operation != test.operation {
				t.Fatalf("status=%d operation=%s body=%s", w.Code, service.operation, w.Body.String())
			}
			if test.operation == "evict" || test.operation == "restore" {
				if service.hash != "abc" {
					t.Fatalf("hash=%q", service.hash)
				}
			}
		})
	}
	for _, err := range []error{replication.ErrConflict, &replication.BlockedError{Reason: "shared payload"}} {
		service := &lifecycleFixture{err: err}
		w := httptest.NewRecorder()
		New(fixture{}, "test", zerolog.Nop(), service).ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/torrents/abc/evict", nil))
		if w.Code != 409 {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	New(fixture{}, "test", zerolog.Nop()).ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/torrents/abc/restore", nil))
	if w.Code != 503 {
		t.Fatalf("disabled status=%d", w.Code)
	}
}

func TestTorrentResponsesExposeCacheState(t *testing.T) {
	service := &transferFixture{record: replication.Record{Hash: "abc", CacheState: "evicted", CacheError: "restore failed"}}
	handler := New(fixture{}, "test", zerolog.Nop(), service)
	for _, path := range []string{"/api/v1/torrents", "/api/v1/torrents/abc"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"cache_state":"evicted"`) || !strings.Contains(w.Body.String(), `"cache_error":"restore failed"`) {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
	}
}
