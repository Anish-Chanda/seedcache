package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/anish-chanda/seedcache/internal/replication"
)

type Adoptions interface {
	Adopt(context.Context, string, bool) (replication.AdoptionResult, error)
	AdoptAll(context.Context, bool) (replication.AdoptionReport, error)
}

func registerAdoptionRoutes(mux *http.ServeMux, transfers Transfers) {
	service, _ := transfers.(Adoptions)
	for _, bulk := range []bool{false, true} {
		path := "/api/v1/torrents/{hash}/adopt"
		if bulk {
			path = "/api/v1/adoptions"
		}
		mux.HandleFunc(path, allowMethod(http.MethodPost, func(w http.ResponseWriter, r *http.Request) {
			if service == nil {
				writeTransferUnavailable(w)
				return
			}
			query := r.URL.Query()
			for key, values := range query {
				if (key != "dry_run" && (key != "apply" || !bulk)) || len(values) != 1 || (values[0] != "true" && values[0] != "false") {
					writeError(w, http.StatusBadRequest, "invalid_adoption", "use boolean dry_run or bulk apply parameters")
					return
				}
			}
			dryRun := query.Get("dry_run") == "true"
			if bulk && dryRun == (query.Get("apply") == "true") {
				writeError(w, http.StatusBadRequest, "invalid_adoption", "bulk adoption requires either dry_run=true or apply=true")
				return
			}
			// Adoption can check a large remote library. Caller cancellation still applies.
			if err := http.NewResponseController(w).SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
				writeError(w, http.StatusInternalServerError, "adoption_failed", "could not extend adoption response deadline")
				return
			}
			if bulk {
				report, err := service.AdoptAll(r.Context(), dryRun)
				if err != nil {
					writeOperationError(w, err, "adoption")
					return
				}
				writeJSON(w, http.StatusOK, report)
				return
			}
			result, err := service.Adopt(r.Context(), r.PathValue("hash"), dryRun)
			if err != nil {
				writeOperationError(w, err, "adoption")
				return
			}
			writeJSON(w, http.StatusOK, result)
		}))
	}
}
