package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/anish-chanda/seedcache/internal/replication"
	"github.com/rs/zerolog"
)

type adoptionServiceFixture struct {
	transferFixture
	hash   string
	dryRun bool
	bulk   bool
	delay  time.Duration
}

func (f *adoptionServiceFixture) Adopt(ctx context.Context, hash string, dry bool) (replication.AdoptionResult, error) {
	f.hash = hash
	f.dryRun = dry
	return replication.AdoptionResult{Hash: hash, Status: "would_adopt"}, nil
}
func (f *adoptionServiceFixture) AdoptAll(ctx context.Context, dry bool) (replication.AdoptionReport, error) {
	f.bulk = true
	f.dryRun = dry
	if f.delay > 0 {
		timer := time.NewTimer(f.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return replication.AdoptionReport{}, ctx.Err()
		}
	}
	return replication.AdoptionReport{DryRun: dry, Results: []replication.AdoptionResult{}, Summary: replication.AdoptionSummary{WouldAdopt: 2}}, nil
}

func TestAdoptionEndpointsAndBulkConfirmation(t *testing.T) {
	for _, tt := range []struct {
		method, path string
		status       int
		bulk, dry    bool
	}{
		{"POST", "/api/v1/torrents/abc/adopt", 200, false, false},
		{"POST", "/api/v1/torrents/abc/adopt?dry_run=true", 200, false, true},
		{"POST", "/api/v1/adoptions?dry_run=true", 200, true, true},
		{"POST", "/api/v1/adoptions?apply=true", 200, true, false},
		{"POST", "/api/v1/adoptions", 400, false, false},
		{"POST", "/api/v1/adoptions?apply=true&dry_run=true", 400, false, false},
		{"POST", "/api/v1/adoptions?dry_run=yes", 400, false, false},
		{"POST", "/api/v1/adoptions?apply=true&apply=false", 400, false, false},
		{"POST", "/api/v1/torrents/abc/adopt?apply=true", 400, false, false},
		{"GET", "/api/v1/torrents/abc/adopt", 405, false, false},
	} {
		t.Run(tt.path, func(t *testing.T) {
			service := &adoptionServiceFixture{}
			w := httptest.NewRecorder()
			New(fixture{}, "test", zerolog.Nop(), service).ServeHTTP(w, httptest.NewRequest(tt.method, tt.path, nil))
			if w.Code != tt.status {
				t.Fatalf("response=%d %s", w.Code, w.Body.String())
			}
			if tt.status == 200 && (service.bulk != tt.bulk || service.dryRun != tt.dry) {
				t.Fatalf("wrong adoption mode: %+v", service)
			}
			if tt.status == 200 && !tt.bulk && service.hash != "abc" {
				t.Fatal("hash not forwarded")
			}
			if tt.status == 400 && (service.bulk || service.hash != "") {
				t.Fatal("invalid request executed adoption")
			}
		})
	}
}

func TestAdoptionDisablesOrdinaryResponseWriteDeadline(t *testing.T) {
	service := &adoptionServiceFixture{delay: 80 * time.Millisecond}
	server := httptest.NewUnstartedServer(New(fixture{}, "test", zerolog.Nop(), service))
	server.Config.WriteTimeout = 10 * time.Millisecond
	server.Start()
	defer server.Close()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/adoptions?dry_run=true", nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("ordinary deadline terminated adoption: %v", err)
	}
	defer response.Body.Close()
	var report replication.AdoptionReport
	if err := json.NewDecoder(response.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || report.Summary.WouldAdopt != 2 {
		t.Fatalf("response=%d %+v", response.StatusCode, report)
	}
}
