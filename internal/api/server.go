package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/anish-chanda/seedcache/internal/replication"
	"github.com/rs/zerolog"
)

var ErrUnavailable = errors.New("qBittorrent is unavailable")
var ErrNotFound = errors.New("torrent not found")

type Discovery interface {
	Status() Status
	Torrents() ([]Torrent, error)
	Detail(context.Context, string) (TorrentDetail, error)
}

type Transfers interface {
	List(context.Context) ([]replication.Record, error)
	Get(context.Context, string) (replication.Record, error)
	Queue(context.Context, string) (replication.Record, error)
	Retry(context.Context, string) (replication.Record, error)
	Summary(context.Context) (replication.Summary, error)
}

type Lifecycle interface {
	Evict(context.Context, string) (replication.Record, error)
	Restore(context.Context, string) (replication.Record, error)
	EvictionPlan(context.Context) (replication.EvictionPlan, error)
}

func New(discovery Discovery, version string, log zerolog.Logger, transferServices ...Transfers) http.Handler {
	var transfers Transfers
	if len(transferServices) > 0 {
		transfers = transferServices[0]
	}
	lifecycle, _ := transfers.(Lifecycle)
	mux := http.NewServeMux()
	registerAdoptionRoutes(mux, transfers)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
	})
	routes := map[string]http.HandlerFunc{
		"/api/v1/health":  func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, Health{Status: "ok"}) },
		"/api/v1/version": func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, Version{Version: version}) },
		"/api/v1/status": func(w http.ResponseWriter, r *http.Request) {
			status := discovery.Status()
			if transfers != nil {
				summary, err := transfers.Summary(r.Context())
				if err != nil {
					writeOperationError(w, err, "replication")
					return
				}
				status.Replication = summary
			}
			writeJSON(w, http.StatusOK, status)
		},
		"/api/v1/torrents": func(w http.ResponseWriter, r *http.Request) {
			torrents, err := discovery.Torrents()
			if err != nil {
				writeError(w, http.StatusServiceUnavailable, "qbittorrent_unavailable", ErrUnavailable.Error())
				return
			}
			if transfers != nil {
				records, err := transfers.List(r.Context())
				if err != nil {
					writeOperationError(w, err, "cache")
					return
				}
				byHash := make(map[string]replication.Record, len(records))
				for _, record := range records {
					byHash[record.Hash] = record
				}
				for i := range torrents {
					applyCache(&torrents[i], byHash[strings.ToLower(torrents[i].Hash)])
				}
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
				if transfers != nil {
					record, err := transfers.Get(r.Context(), detail.Hash)
					if err != nil && !errors.Is(err, replication.ErrNotFound) {
						writeOperationError(w, err, "cache")
						return
					}
					applyCache(&detail.Torrent, record)
				}
				writeJSON(w, http.StatusOK, detail)
			}
		},
	}
	for path, handler := range routes {
		mux.HandleFunc(path, allowMethod(http.MethodGet, handler))
	}
	mux.HandleFunc("/api/v1/evictions/plan", allowMethod(http.MethodGet, func(w http.ResponseWriter, r *http.Request) {
		if lifecycle == nil {
			writeTransferUnavailable(w)
			return
		}
		plan, err := lifecycle.EvictionPlan(r.Context())
		if err != nil {
			writeOperationError(w, err, "eviction")
			return
		}
		writeJSON(w, http.StatusOK, plan)
	}))
	for _, action := range []string{"evict", "restore"} {
		mux.HandleFunc("/api/v1/torrents/{hash}/"+action, allowMethod(http.MethodPost, func(w http.ResponseWriter, r *http.Request) {
			if lifecycle == nil {
				writeTransferUnavailable(w)
				return
			}
			operation := lifecycle.Evict
			if action == "restore" {
				operation = lifecycle.Restore
			}
			record, err := operation(r.Context(), r.PathValue("hash"))
			if err != nil {
				writeOperationError(w, err, "cache")
				return
			}
			writeJSON(w, http.StatusAccepted, record)
		}))
	}
	mux.HandleFunc("/api/v1/transfers", allowMethod(http.MethodGet, func(w http.ResponseWriter, r *http.Request) {
		if transfers == nil {
			writeTransferUnavailable(w)
			return
		}
		records, err := transfers.List(r.Context())
		if err != nil {
			writeOperationError(w, err, "transfer")
			return
		}
		writeJSON(w, http.StatusOK, TransferList{Transfers: records})
	}))
	mux.HandleFunc("/api/v1/transfers/{hash}", allowMethod(http.MethodGet, func(w http.ResponseWriter, r *http.Request) {
		if transfers == nil {
			writeTransferUnavailable(w)
			return
		}
		record, err := transfers.Get(r.Context(), r.PathValue("hash"))
		if err != nil {
			writeOperationError(w, err, "transfer")
			return
		}
		writeJSON(w, http.StatusOK, record)
	}))
	mux.HandleFunc("/api/v1/torrents/{hash}/sync", allowMethod(http.MethodPost, func(w http.ResponseWriter, r *http.Request) {
		if transfers == nil {
			writeTransferUnavailable(w)
			return
		}
		record, err := transfers.Queue(r.Context(), r.PathValue("hash"))
		if err != nil {
			writeOperationError(w, err, "torrent")
			return
		}
		writeJSON(w, http.StatusAccepted, record)
	}))
	mux.HandleFunc("/api/v1/transfers/{hash}/retry", allowMethod(http.MethodPost, func(w http.ResponseWriter, r *http.Request) {
		if transfers == nil {
			writeTransferUnavailable(w)
			return
		}
		record, err := transfers.Retry(r.Context(), r.PathValue("hash"))
		if err != nil {
			writeOperationError(w, err, "transfer")
			return
		}
		writeJSON(w, http.StatusAccepted, record)
	}))
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

func applyCache(torrent *Torrent, record replication.Record) {
	torrent.CacheState = record.CacheState
	if torrent.CacheState == "" {
		torrent.CacheState = "present"
	}
	torrent.CacheError = record.CacheError
	torrent.CacheOperation = record.CacheOperation
	torrent.ReplicationState = record.State
	torrent.VerifiedAt = record.VerifiedAt
}

func allowMethod(method string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be "+method)
			return
		}
		handler(w, r)
	}
}

func writeTransferUnavailable(w http.ResponseWriter) {
	writeError(w, http.StatusServiceUnavailable, "replication_unavailable", "replication service is unavailable")
}

func writeOperationError(w http.ResponseWriter, err error, entity string) {
	var blocked *replication.BlockedError
	switch {
	case errors.Is(err, replication.ErrNotFound):
		writeError(w, http.StatusNotFound, entity+"_not_found", err.Error())
	case errors.Is(err, replication.ErrConflict):
		writeError(w, http.StatusConflict, "transfer_conflict", err.Error())
	case errors.As(err, &blocked):
		writeError(w, http.StatusConflict, "transfer_blocked", err.Error())
	case errors.Is(err, replication.ErrIneligible):
		writeError(w, http.StatusConflict, "torrent_ineligible", err.Error())
	case errors.Is(err, replication.ErrUnavailable):
		writeError(w, http.StatusServiceUnavailable, "replication_unavailable", err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "replication_failed", err.Error())
	}
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

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
