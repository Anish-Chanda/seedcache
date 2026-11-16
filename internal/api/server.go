package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/rs/zerolog"
)

var ErrUnavailable = errors.New("qBittorrent is unavailable")
var ErrNotFound = errors.New("torrent not found")

type Discovery interface {
	Status() Status
	Torrents() ([]Torrent, error)
	Detail(context.Context, string) (TorrentDetail, error)
}

func New(discovery Discovery, version string, log zerolog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
	})
	routes := map[string]http.HandlerFunc{
		"/api/v1/health":  func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, Health{Status: "ok"}) },
		"/api/v1/version": func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, Version{Version: version}) },
		"/api/v1/status":  func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, discovery.Status()) },
		"/api/v1/torrents": func(w http.ResponseWriter, r *http.Request) {
			torrents, err := discovery.Torrents()
			if err != nil {
				writeError(w, http.StatusServiceUnavailable, "qbittorrent_unavailable", ErrUnavailable.Error())
				return
			}
			writeJSON(w, http.StatusOK, TorrentList{Torrents: torrents})
		},
		"/api/v1/torrents/{hash}": func(w http.ResponseWriter, r *http.Request) {
			detail, err := discovery.Detail(r.Context(), r.PathValue("hash"))
			switch {
			case errors.Is(err, ErrNotFound):
				writeError(w, http.StatusNotFound, "torrent_not_found", ErrNotFound.Error())
			case err != nil:
				writeError(w, http.StatusServiceUnavailable, "qbittorrent_unavailable", ErrUnavailable.Error())
			default:
				writeJSON(w, http.StatusOK, detail)
			}
		},
	}
	for path, handler := range routes {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				w.Header().Set("Allow", "GET")
				writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
				return
			}
			handler(w, r)
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		mux.ServeHTTP(sw, r)
		if sw.err != nil {
			log.Error().Err(sw.err).Msg("Write API response failed")
		}
		if r.URL.Path != "/api/v1/health" {
			level := zerolog.InfoLevel
			if sw.status >= 500 {
				level = zerolog.ErrorLevel
			} else if sw.status >= 400 {
				level = zerolog.WarnLevel
			}
			log.WithLevel(level).Dur("duration", time.Since(start)).Msgf("%d %s %s", sw.status, r.Method, r.URL.Path)
		}
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		if sw, ok := w.(*statusWriter); ok {
			sw.err = err
		}
	}
}
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, ErrorResponse{Error: Error{Code: code, Message: message}})
}

type statusWriter struct {
	http.ResponseWriter
	status int
	err    error
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
