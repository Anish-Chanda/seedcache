package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anish-chanda/seedcache/internal/api"
	"github.com/anish-chanda/seedcache/internal/qbittorrent"
	"github.com/rs/zerolog"
)

func TestDiscoveryRecovery(t *testing.T) {
	var unavailable atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if unavailable.Load() {
			w.WriteHeader(503)
			return
		}
		switch r.URL.Path {
		case "/api/v2/auth/login":
			http.SetCookie(w, &http.Cookie{Name: "SID", Value: "session", Path: "/"})
			w.Write([]byte("Ok."))
		case "/api/v2/app/version":
			w.Write([]byte("v5.0.0"))
		case "/api/v2/app/webapiVersion":
			w.Write([]byte("2.11.0"))
		case "/api/v2/app/preferences":
			w.Write([]byte(`{"auto_tmm_enabled":true,"web_ui_password":"never expose"}`))
		case "/api/v2/torrents/info":
			w.Write([]byte(`[{"hash":"abc","name":"example","state":"uploading","total_size":9007199254740993,"uploaded":45,"auto_tmm":true,"completion_on":1700000000,"seeding_time":3600}]`))
		case "/api/v2/torrents/files":
			w.Write([]byte(`[{"index":0,"name":"example/file","size":9007199254740993,"priority":1,"progress":1}]`))
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	client, err := qbittorrent.New(server.URL, "admin", "password")
	if err != nil {
		t.Fatal(err)
	}
	d := newDiscovery(client, zerolog.Nop())
	if _, err := d.Torrents(); !errors.Is(err, api.ErrUnavailable) {
		t.Fatal("expected unavailable before discovery")
	}
	d.poll(context.Background())
	status := d.Status()
	if !status.QBittorrent.Connected || status.TorrentCount != 1 || status.QBittorrent.LastSuccess == nil || status.QBittorrent.AutoTMMEnabled == nil || !*status.QBittorrent.AutoTMMEnabled {
		t.Fatalf("status: %+v", status)
	}
	torrents, err := d.Torrents()
	if err != nil || len(torrents) != 1 || torrents[0].TotalSize != 9007199254740993 || torrents[0].CompletedAt == nil || !torrents[0].AutoTMM {
		t.Fatalf("torrents: %+v, %v", torrents, err)
	}
	detail, err := d.Detail(context.Background(), "abc")
	if err != nil || len(detail.Files) != 1 || detail.Files[0].Size != 9007199254740993 {
		t.Fatalf("detail: %+v, %v", detail, err)
	}
	encoded, err := json.Marshal(status)
	if err != nil || !json.Valid(encoded) {
		t.Fatalf("JSON: %s %v", encoded, err)
	}
	lastSuccess := *status.QBittorrent.LastSuccess
	unavailable.Store(true)
	d.poll(context.Background())
	status = d.Status()
	if status.QBittorrent.Connected || status.QBittorrent.Error == "" || !status.QBittorrent.LastSuccess.Equal(lastSuccess) || status.TorrentCount != 1 {
		t.Fatalf("degraded status: %+v", status)
	}
	if _, err := d.Torrents(); !errors.Is(err, api.ErrUnavailable) {
		t.Fatal("stale data served as current")
	}
	unavailable.Store(false)
	d.poll(context.Background())
	if status = d.Status(); !status.QBittorrent.Connected || status.QBittorrent.Error != "" {
		t.Fatalf("recovery: %+v", status)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { d.run(ctx, time.Hour); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("discovery did not stop")
	}
}
