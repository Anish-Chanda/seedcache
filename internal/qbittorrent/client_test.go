package qbittorrent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testHash = "0123456789abcdef0123456789abcdef01234567"

func newTestClient(t *testing.T, handler http.Handler) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	client, err := New(server.URL, "test-user", "test-password")
	if err != nil {
		server.Close()
		t.Fatalf("New returned error: %v", err)
	}
	t.Cleanup(server.Close)
	return client, server
}

func loginHandler(t *testing.T, calls *atomic.Int32) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("login method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/x-www-form-urlencoded") {
			t.Errorf("login Content-Type = %q", got)
		}
		if got, want := r.Header.Get("Referer"), "http://"+r.Host; got != want {
			t.Errorf("login Referer = %q, want %q", got, want)
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm: %v", err)
		}
		if got := r.Form.Get("username"); got != "test-user" {
			t.Errorf("login username = %q", got)
		}
		if got := r.Form.Get("password"); got != "test-password" {
			t.Errorf("login password = %q", got)
		}
		count := calls.Add(1)
		http.SetCookie(w, &http.Cookie{Name: "SID", Value: fmt.Sprintf("sid-%d", count), Path: "/"})
		_, _ = io.WriteString(w, "Ok.")
	}
}

func TestLoginAcceptsHTTP204AndQBTSessionCookie(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("login method = %s, want POST", r.Method)
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("login parse form: %v", err)
		}
		if r.Form.Get("username") != "test-user" || r.Form.Get("password") != "test-password" {
			t.Error("login credentials mismatch")
		}
		http.SetCookie(w, &http.Cookie{Name: "QBT_SID_8080", Value: "session", Path: "/"})
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("QBT_SID_8080")
		if err != nil || cookie.Value != "session" {
			t.Errorf("expected port-named session cookie, got %v: %v", cookie, err)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = io.WriteString(w, "[]")
	})
	client, _ := newTestClient(t, mux)
	if _, err := client.Torrents(context.Background()); err != nil {
		t.Fatalf("Torrents() after 204 login: %v", err)
	}
}

func TestStopAcceptsHTTP204(t *testing.T) {
	var stopped atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, new(atomic.Int32)))
	mux.HandleFunc("/api/v2/torrents/stop", func(w http.ResponseWriter, r *http.Request) {
		requireMutationRequest(t, r, "stop", testHash)
		stopped.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		if r.URL.Query().Get("filter") == "stopped" && stopped.Load() {
			_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"stoppedUP"}]`)
			return
		}
		_, _ = io.WriteString(w, "[]")
	})
	client, _ := newTestClient(t, mux)
	if err := client.Stop(context.Background(), testHash); err != nil {
		t.Fatalf("Stop() with HTTP 204: %v", err)
	}
}

func TestReadMethodsDecodeDocumentedEndpoints(t *testing.T) {
	var logins atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, &logins))
	mux.HandleFunc("/api/v2/app/version", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", "GET")
		_, _ = io.WriteString(w, "v5.0.2\n")
	})
	mux.HandleFunc("/api/v2/app/webapiVersion", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", "GET")
		_, _ = io.WriteString(w, "2.11.3")
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", "GET")
		if got := r.URL.Query().Get("hashes"); got != "" && got != testHash {
			t.Errorf("torrents/info hashes = %q", got)
		}
		_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","name":"Example","state":"uploading","progress":1,"total_size":4096,"size":2048,"amount_left":0,"content_path":"/data/Example","uploaded":8192,"ratio":2,"seeding_time":3600,"last_activity":1700000012,"save_path":"/data","download_path":"/incomplete","completion_on":1700000000,"auto_tmm":true}]`)
	})
	mux.HandleFunc("/api/v2/torrents/properties", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", "GET")
		if got := r.URL.Query().Get("hash"); got != testHash {
			t.Errorf("properties hash = %q", got)
		}
		_, _ = io.WriteString(w, `{"save_path":"/data","creation_date":100,"completion_date":200,"total_size":4096,"total_uploaded":8192,"share_ratio":2.0,"seeding_time":3600}`)
	})
	mux.HandleFunc("/api/v2/torrents/files", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", "GET")
		if got := r.URL.Query().Get("hash"); got != testHash {
			t.Errorf("files hash = %q", got)
		}
		_, _ = io.WriteString(w, `[{"index":0,"name":"Example/video.mkv","size":4096,"priority":1,"progress":0.5}]`)
	})
	mux.HandleFunc("/api/v2/app/preferences", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", "GET")
		_, _ = io.WriteString(w, `{"auto_tmm_enabled":true,"mail_notification_password":"should-not-be-exposed"}`)
	})
	client, _ := newTestClient(t, mux)

	appVersion, apiVersion, err := client.Versions(context.Background())
	if err != nil || appVersion != "v5.0.2" || apiVersion != "2.11.3" {
		t.Fatalf("Versions() = (%q, %q, %v)", appVersion, apiVersion, err)
	}

	torrents, err := client.Torrents(context.Background())
	if err != nil || len(torrents) != 1 {
		t.Fatalf("Torrents() = (%#v, %v)", torrents, err)
	}
	assertTorrent(t, torrents[0])

	torrent, err := client.Torrent(context.Background(), testHash)
	if err != nil {
		t.Fatalf("Torrent() error: %v", err)
	}
	assertTorrent(t, torrent)

	properties, err := client.Properties(context.Background(), testHash)
	if err != nil {
		t.Fatalf("Properties() error: %v", err)
	}
	if properties.SavePath != "/data" || properties.CreationDate != 100 || properties.CompletionDate != 200 ||
		properties.TotalSize != 4096 || properties.TotalUploaded != 8192 || properties.ShareRatio != 2 || properties.SeedingTime != 3600 {
		t.Errorf("Properties() = %#v", properties)
	}

	files, err := client.Files(context.Background(), testHash)
	if err != nil || len(files) != 1 {
		t.Fatalf("Files() = (%#v, %v)", files, err)
	}
	if got := files[0]; got.Index != 0 || got.Name != "Example/video.mkv" || got.Size != 4096 || got.Priority != 1 || got.Progress != 0.5 {
		t.Errorf("Files()[0] = %#v", got)
	}

	preferences, err := client.Preferences(context.Background())
	if err != nil || preferences.AutoTMMEnabled == nil || !*preferences.AutoTMMEnabled {
		t.Fatalf("Preferences() = (%#v, %v)", preferences, err)
	}
	encoded, err := json.Marshal(preferences)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "should-not-be-exposed") || strings.Contains(string(encoded), "password") {
		t.Errorf("Preferences exposed extra settings: %s", encoded)
	}
	if got := logins.Load(); got != 1 {
		t.Errorf("login calls = %d, want one shared SID", got)
	}
}

func requireReadRequest(t *testing.T, r *http.Request, wantSID, wantMethod string) {
	t.Helper()
	if r.Method != wantMethod {
		t.Errorf("method for %s = %s, want %s", r.URL.Path, r.Method, wantMethod)
	}
	if got, want := r.Header.Get("Referer"), "http://"+r.Host; got != want {
		t.Errorf("Referer for %s = %q, want %q", r.URL.Path, got, want)
	}
	if got, want := r.Header.Get("Origin"), "http://"+r.Host; got != want {
		t.Errorf("Origin for %s = %q, want %q", r.URL.Path, got, want)
	}
	cookie, err := r.Cookie("SID")
	if err != nil || cookie.Value != wantSID {
		t.Errorf("SID for %s = (%v, %v), want %q", r.URL.Path, cookie, err, wantSID)
	}
}

func assertTorrent(t *testing.T, got Torrent) {
	t.Helper()
	if got.Hash != testHash || got.Name != "Example" || got.State != "uploading" || got.Progress != 1 ||
		got.TotalSize != 4096 || got.Uploaded != 8192 || got.Ratio != 2 || got.SeedingTime != 3600 ||
		got.LastActivity != 1700000012 ||
		got.Size != 2048 || got.AmountLeft != 0 || got.ContentPath != "/data/Example" || got.SavePath != "/data" ||
		got.DownloadPath != "/incomplete" || got.CompletionOn != 1700000000 || !got.AutoTMM {
		t.Errorf("Torrent = %#v", got)
	}
}

func TestMutationMethodsPostOneExactHashAndConfirmStates(t *testing.T) {
	var logins atomic.Int32
	var state atomic.Value
	state.Store("uploading")
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, &logins))
	mux.HandleFunc("/api/v2/torrents/stop", func(w http.ResponseWriter, r *http.Request) {
		requireMutationRequest(t, r, "stop", testHash)
		state.Store("stoppedUP")
		_, _ = io.WriteString(w, "Ok.")
	})
	mux.HandleFunc("/api/v2/torrents/start", func(w http.ResponseWriter, r *http.Request) {
		requireMutationRequest(t, r, "start", testHash)
		state.Store("queuedUP")
		_, _ = io.WriteString(w, "Ok.")
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		if got := r.URL.Query().Get("hashes"); got != testHash {
			t.Errorf("torrents/info hashes = %q, want exact hash", got)
		}
		current := state.Load().(string)
		if r.URL.Query().Get("filter") == "stopped" && current != "stoppedUP" && current != "stoppedDL" {
			_, _ = io.WriteString(w, `[]`)
			return
		}
		_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"`+current+`","progress":1,"amount_left":0}]`)
	})
	client, _ := newTestClient(t, mux)

	if err := client.Stop(context.Background(), testHash); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}
	if err := client.Start(context.Background(), testHash); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	if got := logins.Load(); got != 1 {
		t.Errorf("login calls = %d, want one shared SID", got)
	}
}

func TestRecheckWaitsForSuccessfulSelectedFileCheck(t *testing.T) {
	var logins atomic.Int32
	var state atomic.Value
	state.Store("stoppedUP")
	var recheckCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, &logins))
	mux.HandleFunc("/api/v2/torrents/recheck", func(w http.ResponseWriter, r *http.Request) {
		requireMutationRequest(t, r, "recheck", testHash)
		if recheckCalls.Add(1) == 1 {
			state.Store("checkingUP")
		}
		_, _ = io.WriteString(w, "Ok.")
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		if r.URL.Query().Get("filter") == "stopped" {
			if state.Load().(string) == "stoppedUP" {
				_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"stoppedUP"}]`)
				return
			}
			_, _ = io.WriteString(w, `[]`)
			return
		}
		if got := r.URL.Query().Get("hashes"); got != testHash {
			t.Errorf("torrents/info hashes = %q, want exact hash", got)
		}
		if state.Load().(string) == "checkingUP" {
			// Expose the check state once so the client can confirm it started.
			if recheckCalls.Load() > 0 {
				state.Store("stoppedUP")
				_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"checkingUP"}]`)
				return
			}
		}
		_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"`+state.Load().(string)+`"}]`)
	})
	mux.HandleFunc("/api/v2/torrents/files", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		if got := r.URL.Query().Get("hash"); got != testHash {
			t.Errorf("torrents/files hash = %q, want exact hash", got)
		}
		_, _ = io.WriteString(w, `[{"index":0,"name":"selected.bin","size":4,"priority":1,"progress":1},{"index":1,"name":"ignored.bin","size":4,"priority":0,"progress":0},{"index":2,"name":"selected-empty.bin","size":0,"priority":1,"progress":0}]`)
	})
	client, _ := newTestClient(t, mux)

	if err := client.Recheck(context.Background(), testHash); err != nil {
		t.Fatalf("Recheck() error: %v", err)
	}
	if got := recheckCalls.Load(); got != 1 {
		t.Errorf("recheck calls = %d, want 1", got)
	}
}

func TestRecheckAcceptsStoppedIncompleteToCompleteTransition(t *testing.T) {
	var recheckCalls atomic.Int32
	var checked atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, new(atomic.Int32)))
	mux.HandleFunc("/api/v2/torrents/recheck", func(w http.ResponseWriter, r *http.Request) {
		requireMutationRequest(t, r, "recheck", testHash)
		recheckCalls.Add(1)
		checked.Store(true)
		_, _ = io.WriteString(w, "Ok.")
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		if r.URL.Query().Get("filter") == "stopped" {
			_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"stoppedDL"}]`)
			return
		}
		_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"stoppedDL"}]`)
	})
	mux.HandleFunc("/api/v2/torrents/files", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		if checked.Load() {
			_, _ = io.WriteString(w, `[{"index":0,"name":"selected.bin","size":4,"priority":1,"progress":1}]`)
			return
		}
		_, _ = io.WriteString(w, `[{"index":0,"name":"selected.bin","size":4,"priority":1,"progress":0}]`)
	})
	client, _ := newTestClient(t, mux)

	if err := client.Recheck(context.Background(), testHash); err != nil {
		t.Fatalf("Recheck() error: %v", err)
	}
	if got := recheckCalls.Load(); got != 1 {
		t.Errorf("recheck calls = %d, want 1", got)
	}
}

func TestRecheckUsesCheckingFilterWhenStatePulseIsMissed(t *testing.T) {
	var checking atomic.Bool
	var checkingFilterCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, new(atomic.Int32)))
	mux.HandleFunc("/api/v2/torrents/recheck", func(w http.ResponseWriter, r *http.Request) {
		requireMutationRequest(t, r, "recheck", testHash)
		checking.Store(true)
		_, _ = io.WriteString(w, "Ok.")
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		if got := r.URL.Query().Get("hashes"); got != testHash {
			t.Errorf("torrents/info hashes = %q, want exact hash", got)
		}
		switch r.URL.Query().Get("filter") {
		case "stopped":
			_, _ = io.WriteString(w, "[{\"hash\":\""+testHash+"\",\"state\":\"stoppedUP\"}]")
		case "checking":
			checkingFilterCalls.Add(1)
			if checking.Swap(false) {
				// The qB checking filter catches a check that did not appear in
				// the per-torrent state response.
				_, _ = io.WriteString(w, "[{\"hash\":\""+testHash+"\",\"state\":\"stoppedUP\"}]")
				return
			}
			_, _ = io.WriteString(w, "[]")
		default:
			_, _ = io.WriteString(w, "[{\"hash\":\""+testHash+"\",\"state\":\"stoppedUP\"}]")
		}
	})
	mux.HandleFunc("/api/v2/torrents/files", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		_, _ = io.WriteString(w, "[{\"index\":0,\"name\":\"selected.bin\",\"size\":4,\"priority\":1,\"progress\":1}]")
	})
	client, _ := newTestClient(t, mux)

	if err := client.Recheck(context.Background(), testHash); err != nil {
		t.Fatalf("Recheck() error: %v", err)
	}
	if got := checkingFilterCalls.Load(); got < 3 {
		t.Errorf("checking filter calls = %d, want baseline, start, and finish polls", got)
	}
}

func TestRecheckRejectsIncompleteSelectedFile(t *testing.T) {
	var state atomic.Value
	state.Store("stoppedUP")
	var observedChecking atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, new(atomic.Int32)))
	mux.HandleFunc("/api/v2/torrents/recheck", func(w http.ResponseWriter, r *http.Request) {
		requireMutationRequest(t, r, "recheck", testHash)
		state.Store("checkingUP")
		_, _ = io.WriteString(w, "Ok.")
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		if r.URL.Query().Get("filter") == "stopped" {
			if state.Load().(string) == "stoppedUP" {
				_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"stoppedUP"}]`)
				return
			}
			_, _ = io.WriteString(w, `[]`)
			return
		}
		current := state.Load().(string)
		if current == "checkingUP" && observedChecking.CompareAndSwap(false, true) {
			_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"checkingUP"}]`)
			return
		}
		state.Store("stoppedUP")
		_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"stoppedUP"}]`)
	})
	mux.HandleFunc("/api/v2/torrents/files", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		_, _ = io.WriteString(w, `[{"index":0,"name":"selected.bin","size":4,"priority":1,"progress":0.5}]`)
	})
	client, _ := newTestClient(t, mux)

	err := client.Recheck(context.Background(), testHash)
	if err == nil || !strings.Contains(err.Error(), "selected file") {
		t.Fatalf("Recheck() error = %v, want selected-file error", err)
	}
}

func TestMutationRejectsNonExactHashesWithoutNetworkCalls(t *testing.T) {
	var calls atomic.Int32
	client, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.NotFound(w, r)
	}))
	for _, hash := range []string{"", "all", "|" + testHash, testHash + "|", "  " + testHash, strings.Repeat("g", 40), strings.Repeat("a", 41)} {
		if err := client.Stop(context.Background(), hash); err == nil {
			t.Errorf("Stop(%q) unexpectedly succeeded", hash)
		}
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("invalid hashes caused %d HTTP calls, want zero", got)
	}
}

func TestMutationRetriesExpiredSID(t *testing.T) {
	var logins atomic.Int32
	var stopCalls atomic.Int32
	var state atomic.Value
	state.Store("uploading")
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, &logins))
	mux.HandleFunc("/api/v2/torrents/stop", func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("SID")
		if err != nil {
			t.Errorf("stop missing SID: %v", err)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		requireMutationRequest(t, r, "stop", testHash)
		if stopCalls.Add(1) == 1 {
			if cookie.Value != "sid-1" {
				t.Errorf("first stop SID = %q, want sid-1", cookie.Value)
			}
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if cookie.Value != "sid-2" {
			t.Errorf("retried stop SID = %q, want sid-2", cookie.Value)
		}
		state.Store("stoppedUP")
		_, _ = io.WriteString(w, "Ok.")
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-2", http.MethodGet)
		if r.URL.Query().Get("filter") != "stopped" {
			t.Errorf("filter = %q, want stopped", r.URL.Query().Get("filter"))
		}
		if state.Load().(string) == "stoppedUP" {
			_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"stoppedUP"}]`)
			return
		}
		_, _ = io.WriteString(w, `[]`)
	})
	client, _ := newTestClient(t, mux)
	if err := client.Stop(context.Background(), testHash); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}
	if got := logins.Load(); got != 2 {
		t.Errorf("login calls = %d, want initial login and refresh", got)
	}
	if got := stopCalls.Load(); got != 2 {
		t.Errorf("stop calls = %d, want original request and retry", got)
	}
}

func TestRecheckRejectsHTTP200WithoutObservedCheck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var recheckCalls atomic.Int32
	var recheckPolls atomic.Int32
	var state atomic.Value
	state.Store("stoppedUP")
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, new(atomic.Int32)))
	mux.HandleFunc("/api/v2/torrents/recheck", func(w http.ResponseWriter, r *http.Request) {
		requireMutationRequest(t, r, "recheck", testHash)
		recheckCalls.Add(1)
		_, _ = io.WriteString(w, "Ok.")
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		if recheckCalls.Load() > 0 && recheckPolls.Add(1) == 2 {
			cancel()
		}
		if r.URL.Query().Get("filter") == "stopped" {
			_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"stoppedUP"}]`)
			return
		}
		_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"`+state.Load().(string)+`"}]`)
	})
	mux.HandleFunc("/api/v2/torrents/files", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		_, _ = io.WriteString(w, `[{"index":0,"name":"selected.bin","size":4,"priority":1,"progress":1}]`)
	})
	client, _ := newTestClient(t, mux)
	err := client.Recheck(ctx, testHash)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Recheck() error = %v, want cancellation without confirmed recheck", err)
	}
	if got := recheckCalls.Load(); got != 1 {
		t.Fatalf("recheck calls = %d, want one acknowledged request", got)
	}
	if got := recheckPolls.Load(); got < 2 {
		t.Fatalf("recheck polls = %d, want continued polling after unchanged state", got)
	}
}

func TestStopRequiresConfirmedStoppedFilter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stopCalls atomic.Int32
	var state atomic.Value
	state.Store("uploading")
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, new(atomic.Int32)))
	mux.HandleFunc("/api/v2/torrents/stop", func(w http.ResponseWriter, r *http.Request) {
		requireMutationRequest(t, r, "stop", testHash)
		stopCalls.Add(1)
		_, _ = io.WriteString(w, "Ok.")
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		if r.URL.Query().Get("filter") == "stopped" {
			_, _ = io.WriteString(w, `[]`)
			return
		}
		cancel()
		_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"`+state.Load().(string)+`"}]`)
	})
	client, _ := newTestClient(t, mux)
	if err := client.Stop(ctx, testHash); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop() error = %v, want cancellation without confirmed stop", err)
	}
	if got := stopCalls.Load(); got != 1 {
		t.Fatalf("stop calls = %d, want one acknowledged request", got)
	}
}

func TestStopUsesStoppedFilterForMissingFilesState(t *testing.T) {
	var state atomic.Value
	state.Store("missingFiles")
	var stopped atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, new(atomic.Int32)))
	mux.HandleFunc("/api/v2/torrents/stop", func(w http.ResponseWriter, r *http.Request) {
		requireMutationRequest(t, r, "stop", testHash)
		stopped.Store(true)
		_, _ = io.WriteString(w, "Ok.")
	})
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", http.MethodGet)
		if r.URL.Query().Get("hashes") != testHash {
			t.Errorf("hashes = %q, want exact hash", r.URL.Query().Get("hashes"))
		}
		if r.URL.Query().Get("filter") == "stopped" {
			if stopped.Load() {
				_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"missingFiles"}]`)
				return
			}
			_, _ = io.WriteString(w, `[]`)
			return
		}
		_, _ = io.WriteString(w, `[{"hash":"`+testHash+`","state":"`+state.Load().(string)+`"}]`)
	})
	client, _ := newTestClient(t, mux)
	if err := client.Stop(context.Background(), testHash); err != nil {
		t.Fatalf("Stop() error for missingFiles torrent: %v", err)
	}
	if stopped, err := client.Stopped(context.Background(), testHash); err != nil || !stopped {
		t.Fatalf("Stopped() = (%t, %v), want (true, nil)", stopped, err)
	}
}

func requireMutationRequest(t *testing.T, r *http.Request, action, wantHash string) {
	t.Helper()
	if r.Method != http.MethodPost {
		t.Errorf("%s method = %s, want POST", action, r.Method)
	}
	if r.URL.Path != "/api/v2/torrents/"+action {
		t.Errorf("mutation path = %q, want %q", r.URL.Path, "/api/v2/torrents/"+action)
	}
	if r.URL.RawQuery != "" {
		t.Errorf("mutation query = %q, want no query", r.URL.RawQuery)
	}
	if got, want := r.Header.Get("Content-Type"), "application/x-www-form-urlencoded"; got != want {
		t.Errorf("mutation Content-Type = %q, want %q", got, want)
	}
	if got, want := r.Header.Get("Referer"), "http://"+r.Host; got != want {
		t.Errorf("mutation Referer = %q, want %q", got, want)
	}
	if got, want := r.Header.Get("Origin"), "http://"+r.Host; got != want {
		t.Errorf("mutation Origin = %q, want %q", got, want)
	}
	if _, err := r.Cookie("SID"); err != nil {
		t.Errorf("mutation missing SID: %v", err)
	}
	if err := r.ParseForm(); err != nil {
		t.Errorf("mutation ParseForm: %v", err)
	}
	if got := r.PostForm.Get("hashes"); got != wantHash {
		t.Errorf("mutation hashes = %q, want exact hash %q", got, wantHash)
	}
	if got := len(r.PostForm["hashes"]); got != 1 {
		t.Errorf("mutation hashes values = %d, want one", got)
	}
}

func TestExpiredSIDRetriesOnce(t *testing.T) {
	var logins atomic.Int32
	var apiCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, &logins))
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		apiCalls.Add(1)
		cookie, err := r.Cookie("SID")
		if err != nil {
			t.Errorf("missing SID: %v", err)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if cookie.Value == "sid-1" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if cookie.Value != "sid-2" {
			t.Errorf("retry SID = %q, want sid-2", cookie.Value)
		}
		_, _ = io.WriteString(w, "[]")
	})
	client, _ := newTestClient(t, mux)

	if _, err := client.Torrents(context.Background()); err != nil {
		t.Fatalf("Torrents() error: %v", err)
	}
	if got := logins.Load(); got != 2 {
		t.Errorf("login calls = %d, want initial login plus one refresh", got)
	}
	if got := apiCalls.Load(); got != 2 {
		t.Errorf("API calls = %d, want original plus one retry", got)
	}
}

func TestConcurrentExpiredSIDRefreshIsCoalesced(t *testing.T) {
	const requests = 8
	var logins atomic.Int32
	var oldSIDRequests atomic.Int32
	var apiCalls atomic.Int32
	allOldSIDRequests := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, &logins))
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		apiCalls.Add(1)
		cookie, err := r.Cookie("SID")
		if err != nil {
			t.Errorf("missing SID: %v", err)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if cookie.Value == "sid-1" {
			if oldSIDRequests.Add(1) == requests {
				close(allOldSIDRequests)
			}
			<-allOldSIDRequests
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if cookie.Value != "sid-2" {
			t.Errorf("retry SID = %q, want sid-2", cookie.Value)
		}
		_, _ = io.WriteString(w, "[]")
	})
	client, _ := newTestClient(t, mux)

	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, requests)
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := client.Torrents(context.Background())
			errCh <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Errorf("concurrent Torrents() error: %v", err)
		}
	}
	if got := logins.Load(); got != 2 {
		t.Errorf("login calls = %d, want one initial login and one shared refresh", got)
	}
	if got, want := apiCalls.Load(), int32(requests*2); got != want {
		t.Errorf("API calls = %d, want %d", got, want)
	}
}

func TestNewRejectsBaseURLsWithCredentialsOrQuery(t *testing.T) {
	for _, baseURL := range []string{
		"",
		"/relative/path",
		"ftp://example.test",
		"http://user:password@example.test",
		"http://example.test/?token=secret",
		"http://example.test/#fragment",
	} {
		t.Run(baseURL, func(t *testing.T) {
			if _, err := New(baseURL, "user", "password"); err == nil {
				t.Fatalf("New(%q) succeeded, want validation error", baseURL)
			} else if strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "secret") {
				t.Errorf("validation error leaked input: %v", err)
			}
		})
	}
}

func TestMissingHashAndNotFoundResponses(t *testing.T) {
	var logins atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, &logins))
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", "GET")
		_, _ = io.WriteString(w, "[]")
	})
	mux.HandleFunc("/api/v2/torrents/properties", func(w http.ResponseWriter, r *http.Request) {
		requireReadRequest(t, r, "sid-1", "GET")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "private proxy detail")
	})
	client, _ := newTestClient(t, mux)

	if _, err := client.Torrent(context.Background(), " "); !errors.Is(err, ErrNotFound) {
		t.Errorf("Torrent(empty hash) error = %v, want ErrNotFound", err)
	}
	if got := logins.Load(); got != 0 {
		t.Errorf("empty hash made %d HTTP login requests", got)
	}
	if _, err := client.Torrent(context.Background(), testHash); !errors.Is(err, ErrNotFound) {
		t.Errorf("Torrent(unknown hash) error = %v, want ErrNotFound", err)
	}
	if _, err := client.Properties(context.Background(), testHash); !errors.Is(err, ErrNotFound) {
		t.Errorf("Properties(unknown hash) error = %v, want ErrNotFound", err)
	} else if strings.Contains(err.Error(), "private proxy detail") {
		t.Errorf("not-found error leaked response body: %v", err)
	}
}

func TestHTTPErrorKeepsStatusWithoutResponseBody(t *testing.T) {
	var logins atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, &logins))
	mux.HandleFunc("/api/v2/app/version", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "proxy-secret-value")
	})
	client, _ := newTestClient(t, mux)

	_, _, err := client.Versions(context.Background())
	var statusErr *HTTPError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("Versions() error = %#v, want HTTPError 503", err)
	}
	if strings.Contains(err.Error(), "proxy-secret-value") || strings.Contains(err.Error(), "test-password") {
		t.Errorf("status error leaked data: %v", err)
	}
}

func TestLoginFailureDoesNotExposeCredentialsOrBody(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), url.QueryEscape("test-password")) {
			t.Errorf("login form did not encode password: %q", body)
		}
		_, _ = io.WriteString(w, "login failed: test-password and internal detail")
	})
	client, _ := newTestClient(t, mux)
	_, err := client.Torrents(context.Background())
	if err == nil {
		t.Fatal("Torrents() succeeded without a SID cookie")
	}
	if strings.Contains(err.Error(), "test-password") || strings.Contains(err.Error(), "internal detail") {
		t.Errorf("authentication error leaked details: %v", err)
	}
}

func TestRedirectIsNotFollowed(t *testing.T) {
	var redirectTargetCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/steal", http.StatusFound)
	})
	mux.HandleFunc("/steal", func(w http.ResponseWriter, _ *http.Request) {
		redirectTargetCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	client, _ := newTestClient(t, mux)
	_, err := client.Torrents(context.Background())
	var statusErr *HTTPError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusFound {
		t.Fatalf("Torrents() error = %v, want HTTPError 302", err)
	}
	if got := redirectTargetCalls.Load(); got != 0 {
		t.Errorf("followed login redirect %d times", got)
	}
}

func TestContextCancellationIsReturned(t *testing.T) {
	var logins atomic.Int32
	started := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/auth/login", loginHandler(t, &logins))
	mux.HandleFunc("/api/v2/torrents/info", func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	})
	client, _ := newTestClient(t, mux)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Torrents(ctx)
		errCh <- err
	}()
	select {
	case <-started:
		cancel()
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("API request did not start")
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Torrents() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Torrents() did not return after cancellation")
	}
}

func TestTransportErrorDoesNotIncludeBaseURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	client, err := New(server.URL, "user", "password")
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	server.Close()

	_, err = client.Torrents(context.Background())
	if err == nil {
		t.Fatal("Torrents() succeeded against a closed server")
	}
	if strings.Contains(err.Error(), server.URL) || strings.Contains(err.Error(), "password") {
		t.Errorf("transport error leaked connection details: %v", err)
	}
	var networkErr *net.OpError
	if !errors.As(err, &networkErr) {
		t.Errorf("transport error did not preserve its network cause: %v", err)
	}
}
