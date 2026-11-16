package api

import (
	"context"
	"encoding/json"
	"github.com/rs/zerolog"
	"net/http/httptest"
	"testing"
)

type fixture struct{ unavailable bool }

func (f fixture) Status() Status { return Status{QBittorrent: Connection{Connected: !f.unavailable}} }
func (f fixture) Torrents() ([]Torrent, error) {
	if f.unavailable {
		return nil, ErrUnavailable
	}
	return []Torrent{{Hash: "abc", Name: "example", TotalSize: 9007199254740993}}, nil
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
