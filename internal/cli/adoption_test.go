package cli

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestAdoptCommandRejectsAmbiguousArguments(t *testing.T) {
	for _, args := range [][]string{{"adopt"}, {"adopt", "--all"}, {"adopt", "hash", "--all", "--apply"}, {"adopt", "hash", "--apply"}, {"adopt", "hash", "--dry-run", "--apply"}, {"adopt", "--all", "--dry-run", "--apply"}, {"adopt", "a", "b"}} {
		withTransport(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("invalid adoption made HTTP request")
			return nil, nil
		}))
		if _, err := execute(t, New("test", nil), args...); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

func TestAdoptCommandModesAndJSON(t *testing.T) {
	for _, tt := range []struct {
		args              []string
		path, query, body string
	}{
		{[]string{"adopt", "abc"}, "/api/v1/torrents/abc/adopt", "", `{"hash":"abc","status":"adopted"}`},
		{[]string{"adopt", "abc", "--dry-run"}, "/api/v1/torrents/abc/adopt", "dry_run=true", `{"hash":"abc","status":"would_adopt"}`},
		{[]string{"adopt", "--all", "--dry-run"}, "/api/v1/adoptions", "dry_run=true", `{"dry_run":true,"results":[],"summary":{"would_adopt":3}}`},
		{[]string{"adopt", "--all", "--apply"}, "/api/v1/adoptions", "apply=true", `{"results":[{"hash":"abc","status":"blocked","error":"partial local payload"}],"summary":{"blocked":1}}`},
	} {
		t.Run(strings.Join(tt.args, " "), func(t *testing.T) {
			withTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method != "POST" || r.URL.Path != tt.path || r.URL.RawQuery != tt.query {
					t.Fatalf("request %s %s", r.Method, r.URL)
				}
				if _, limited := r.Context().Deadline(); limited {
					t.Fatal("ordinary short HTTP timeout applied to adoption")
				}
				return apiResponse(200, tt.body), nil
			}))
			args := append([]string{"--json"}, tt.args...)
			output, err := execute(t, New("test", nil), args...)
			if err != nil || !json.Valid([]byte(output)) {
				t.Fatalf("JSON=%s err=%v", output, err)
			}
			output, err = execute(t, New("test", nil), tt.args...)
			if err != nil || output == "" {
				t.Fatalf("human=%s err=%v", output, err)
			}
		})
	}
}
