package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anish-chanda/seedcache/internal/api"
	"github.com/anish-chanda/seedcache/internal/replication"
	"github.com/spf13/cobra"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func withTransport(t *testing.T, transport http.RoundTripper) {
	t.Helper()
	previous := http.DefaultTransport
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = previous })
}

func apiResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func execute(t *testing.T, command *cobra.Command, args ...string) (string, error) {
	t.Helper()
	var output strings.Builder
	command.SetOut(&output)
	command.SetArgs(args)
	err := command.Execute()
	return output.String(), err
}

func TestStatusJSONUsesStatusEndpoint(t *testing.T) {
	started := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	withTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/status" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			return apiResponse(http.StatusNotFound, `{"error":{"code":"not_found","message":"endpoint not found"}}`), nil
		}
		body, _ := json.Marshal(api.Status{
			StartedAt: started,
			QBittorrent: api.Connection{
				Connected:  true,
				Version:    "4.6.7",
				APIVersion: "2.11.3",
			},
			TorrentCount: 3,
		})
		return apiResponse(http.StatusOK, string(body)), nil
	}))

	command := New("test", nil)
	output, err := execute(t, command, "--server", "http://seedcache.test", "--json", "status")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var got api.Status
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("output is not status JSON: %v\n%s", err, output)
	}
	if !got.StartedAt.Equal(started) || got.TorrentCount != 3 || !got.QBittorrent.Connected {
		t.Fatalf("unexpected status: %+v", got)
	}
}

func TestTransfersJSONUsesTransfersEndpoint(t *testing.T) {
	withTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/transfers" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			return apiResponse(http.StatusNotFound, `{"error":{"code":"not_found","message":"endpoint not found"}}`), nil
		}
		return apiResponse(http.StatusOK, `{"transfers":[{"hash":"abc","state":"replicated","manifest":{"files":[],"total_size":1024},"attempts":1,"verified_at":"2025-01-02T03:04:05Z"}]}`), nil
	}))

	output, err := execute(t, New("test", nil), "--server", "http://seedcache.test", "transfers", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var got api.TransferList
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("output is not transfer JSON: %v\n%s", err, output)
	}
	if len(got.Transfers) != 1 || got.Transfers[0].Hash != "abc" || got.Transfers[0].State != replication.StateReplicated {
		t.Fatalf("unexpected transfers: %+v", got)
	}
}

func TestTransfersHumanOutputShowsStateAndVerification(t *testing.T) {
	withTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/transfers" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			return apiResponse(http.StatusNotFound, `{"error":{"code":"not_found","message":"endpoint not found"}}`), nil
		}
		return apiResponse(http.StatusOK, `{"transfers":[{"hash":"abc","state":"blocked","manifest":{"files":[],"total_size":1024},"attempts":1,"last_attempt_at":"2025-01-02T03:04:05Z","last_error":"unsafe path"}]}`), nil
	}))

	output, err := execute(t, New("test", nil), "--server", "http://seedcache.test", "transfers")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, expected := range []string{"HASH", "SIZE", "STATE", "ATTEMPTS", "LAST ATTEMPT", "VERIFIED", "ERROR", "abc", "blocked", "unsafe path", "no"} {
		if !strings.Contains(output, expected) {
			t.Errorf("output does not contain %q:\n%s", expected, output)
		}
	}
}

func TestTransferShowHumanOutput(t *testing.T) {
	withTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/transfers/abc" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			return apiResponse(http.StatusNotFound, `{"error":{"code":"not_found","message":"endpoint not found"}}`), nil
		}
		return apiResponse(http.StatusOK, `{"hash":"abc","state":"failed","manifest":{"files":[{"path":"movie.mkv","size":2048}],"total_size":2048},"attempts":2,"last_error":"destination unavailable"}`), nil
	}))

	output, err := execute(t, New("test", nil), "--server", "http://seedcache.test", "transfers", "show", "abc")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, expected := range []string{"Hash: abc", "State: failed", "Selected size:", "Selected files: 1", "Attempts: 2", "Verification: not verified", "Last error: destination unavailable"} {
		if !strings.Contains(output, expected) {
			t.Errorf("output does not contain %q:\n%s", expected, output)
		}
	}
	if strings.Contains(output, "%") {
		t.Fatalf("output shows unsupported progress: %s", output)
	}
}

func TestSyncAndRetryUsePOSTAndEmitJSON(t *testing.T) {
	for _, tt := range []struct {
		name string
		path string
	}{
		{name: "sync", path: "/api/v1/torrents/abc/sync"},
		{name: "retry", path: "/api/v1/transfers/abc/retry"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			withTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodPost || r.URL.Path != tt.path {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					return apiResponse(http.StatusNotFound, `{"error":{"code":"not_found","message":"endpoint not found"}}`), nil
				}
				return apiResponse(http.StatusAccepted, `{"hash":"abc","state":"pending","manifest":{"files":[],"total_size":0}}`), nil
			}))

			output, err := execute(t, New("test", nil), "--server", "http://seedcache.test", tt.name, "abc", "--json")
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			var got replication.Record
			if err := json.Unmarshal([]byte(output), &got); err != nil || got.Hash != "abc" || got.State != replication.StatePending {
				t.Fatalf("output is not accepted transfer JSON: %v\n%s", err, output)
			}
		})
	}
}

func TestTorrentsHumanOutputIncludesFullHashAndSummary(t *testing.T) {
	const hash = "0123456789abcdef0123456789abcdef01234567"
	withTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/v1/torrents" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			return apiResponse(http.StatusNotFound, `{"error":{"code":"not_found","message":"endpoint not found"}}`), nil
		}
		return apiResponse(http.StatusOK, `{"torrents":[{"hash":"`+hash+`","name":"Ubuntu ISO","state":"uploading","progress":1,"total_size":1073741824,"uploaded":2147483648,"ratio":2,"seeding_seconds":3600,"save_path":"/downloads","auto_tmm":true}]}`), nil
	}))

	command := New("test", nil)
	output, err := execute(t, command, "torrents", "--server", "http://seedcache.test")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, expected := range []string{"NAME", "SIZE", "PROGRESS", "RATIO", "STATE", "HASH", "Ubuntu ISO", "100.0%", "2.00", "uploading", hash} {
		if !strings.Contains(output, expected) {
			t.Errorf("output does not contain %q:\n%s", expected, output)
		}
	}
}

func TestTorrentShowJSONIncludesManifestAndEscapesHash(t *testing.T) {
	const hash = "abc/def"
	withTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.EscapedPath() != "/api/v1/torrents/abc%2Fdef" {
			t.Errorf("path = %q, want escaped hash", r.URL.EscapedPath())
			return apiResponse(http.StatusNotFound, `{"error":{"code":"not_found","message":"endpoint not found"}}`), nil
		}
		return apiResponse(http.StatusOK, `{"hash":"abc/def","name":"bundle","state":"paused","progress":0.5,"total_size":100,"uploaded":10,"ratio":0.1,"seeding_seconds":20,"save_path":"/data","auto_tmm":false,"files":[{"index":0,"path":"a.txt","size":100,"priority":1,"progress":0.5}]}`), nil
	}))

	command := New("test", nil)
	output, err := execute(t, command, "--server", "http://seedcache.test", "torrents", "show", hash, "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var got api.TorrentDetail
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("output is not torrent detail JSON: %v\n%s", err, output)
	}
	if got.Hash != hash || len(got.Files) != 1 || got.Files[0].Path != "a.txt" {
		t.Fatalf("unexpected detail: %+v", got)
	}
}

func TestAPIErrorEnvelopeIsShown(t *testing.T) {
	withTransport(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
		return apiResponse(http.StatusServiceUnavailable, `{"error":{"code":"qbittorrent_unavailable","message":"qBittorrent is unavailable"}}`), nil
	}))

	command := New("test", nil)
	output, err := execute(t, command, "--server", "http://seedcache.test", "torrents")
	if err == nil {
		t.Fatal("Execute() error = nil, want API error")
	}
	if !strings.Contains(err.Error(), "qbittorrent_unavailable") || !strings.Contains(err.Error(), "qBittorrent is unavailable") {
		t.Fatalf("error = %q", err)
	}
	if output != "" {
		t.Fatalf("output = %q, want empty output on API failure", output)
	}
}

func TestConnectionFailureIsUseful(t *testing.T) {
	withTransport(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("connection refused")
	}))

	command := New("test", nil)
	_, err := execute(t, command, "--server", "http://seedcache.test", "status")
	if err == nil || !strings.Contains(err.Error(), "request seedcache API") {
		t.Fatalf("Execute() error = %v, want useful connection error", err)
	}
}

func TestVersionIsLocalAndJSON(t *testing.T) {
	command := New("1.2.3", nil)
	output, err := execute(t, command, "--server", "not a URL", "version", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var got api.Version
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("output is not version JSON: %v\n%s", err, output)
	}
	if got.Version != "1.2.3" {
		t.Fatalf("version = %q", got.Version)
	}
}

func TestConfigValidateJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	config := "qbittorrent:\n  url: http://localhost:8080\n  username: test\n  password: secret\n"
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	command := New("test", nil)
	output, err := execute(t, command, "--config", path, "--json", "config", "validate")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var result struct {
		Valid bool `json:"valid"`
	}
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("output is not validation JSON: %v\n%s", err, output)
	}
	if !result.Valid {
		t.Fatalf("validation result = %+v", result)
	}
}

func TestDaemonReceivesConfigAndCommandContext(t *testing.T) {
	type contextKey string
	ctx := context.WithValue(context.Background(), contextKey("test"), "value")
	var gotPath string
	var gotContextValue any
	command := New("test", func(received context.Context, path string) error {
		gotPath = path
		gotContextValue = received.Value(contextKey("test"))
		return nil
	})
	command.SetContext(ctx)
	if _, err := execute(t, command, "--config", "custom.yaml", "daemon"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if gotPath != "custom.yaml" || gotContextValue != "value" {
		t.Fatalf("runner received path %q and context value %v", gotPath, gotContextValue)
	}
}

func TestServerURLFromEnvironmentAndFlag(t *testing.T) {
	t.Setenv("SEEDCACHE_API_URL", "http://environment.test")
	for _, tt := range []struct {
		name, host string
		args       []string
	}{
		{name: "environment", host: "environment.test", args: []string{"status"}},
		{name: "flag", host: "flag.test", args: []string{"--server", "http://flag.test", "status"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			withTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Host != tt.host {
					t.Errorf("host = %s, want %s", r.URL.Host, tt.host)
				}
				return apiResponse(http.StatusOK, `{"qbittorrent":{"connected":false,"error":"connection refused"},"torrent_count":0}`), nil
			}))
			output, err := execute(t, New("test", nil), tt.args...)
			if err != nil || !strings.Contains(output, "disconnected") || !strings.Contains(output, "connection refused") {
				t.Fatalf("output %q error %v", output, err)
			}
		})
	}
}

func TestCacheCommandsUseDaemonAPI(t *testing.T) {
	for _, test := range []struct {
		args               []string
		method, path, body string
	}{
		{[]string{"evict", "abc"}, "POST", "/api/v1/torrents/abc/evict", `{"hash":"abc","cache_state":"present","cache_operation":"evict"}`},
		{[]string{"restore", "abc"}, "POST", "/api/v1/torrents/abc/restore", `{"hash":"abc","cache_state":"evicted","cache_operation":"restore"}`},
		{[]string{"evict", "plan"}, "GET", "/api/v1/evictions/plan", `{"enabled":false,"free_percent":5,"low_free_percent":10,"target_free_percent":20,"candidates":[]}`},
	} {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			withTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method != test.method || r.URL.Path != test.path {
					t.Fatalf("request=%s %s", r.Method, r.URL.Path)
				}
				return apiResponse(202, test.body), nil
			}))
			args := append([]string{"--json"}, test.args...)
			output, err := execute(t, New("test", nil), args...)
			if err != nil || !json.Valid([]byte(output)) {
				t.Fatalf("output=%s error=%v", output, err)
			}
			output, err = execute(t, New("test", nil), test.args...)
			if err != nil || output == "" {
				t.Fatalf("human output=%s error=%v", output, err)
			}
		})
	}
}
