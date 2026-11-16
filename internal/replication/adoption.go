package replication

import (
	"context"
	"errors"
	"fmt"
	"github.com/anish-chanda/seedcache/internal/qbittorrent"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type AdoptionResult struct {
	Hash   string  `json:"hash"`
	Status string  `json:"status"`
	Error  string  `json:"error,omitempty"`
	Record *Record `json:"record,omitempty"`
}

type AdoptionSummary struct {
	Adopted         int `json:"adopted"`
	WouldAdopt      int `json:"would_adopt"`
	AlreadyAdopted  int `json:"already_adopted"`
	AlreadyVerified int `json:"already_verified"`
	MissingRemote   int `json:"missing_remote"`
	Blocked         int `json:"blocked"`
	Errors          int `json:"errors"`
}

type AdoptionReport struct {
	DryRun  bool             `json:"dry_run"`
	Results []AdoptionResult `json:"results"`
	Summary AdoptionSummary  `json:"summary"`
}

type remoteAdoptionError struct{ error }

func (e *remoteAdoptionError) Unwrap() error { return e.error }

func (e *Engine) Adopt(ctx context.Context, hash string, dryRun bool) (AdoptionResult, error) {
	hash = strings.ToLower(strings.TrimSpace(hash))
	if err := e.acquireCatalog(ctx); err != nil {
		return AdoptionResult{}, err
	}
	reservation, err := e.reserveAdoptions(ctx, []string{hash}, nil)
	e.releaseCatalog()
	if err != nil {
		return AdoptionResult{Hash: hash}, err
	}
	defer e.releaseAdoptions(reservation)
	return e.adoptReserved(ctx, hash, dryRun, reservation)
}

func (e *Engine) adoptReserved(ctx context.Context, hash string, dryRun bool, reservation *adoptionReservation) (AdoptionResult, error) {
	result := AdoptionResult{Hash: strings.ToLower(strings.TrimSpace(hash))}
	if !validLifecycleHash(result.Hash) {
		return result, fmt.Errorf("%w: torrent hash must be 40 or 64 hexadecimal characters", ErrConflict)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := reservation.conflicts[result.Hash]; err != nil {
		return result, err
	}
	e.mu.Lock()
	active := e.activeHash == result.Hash || e.activeCacheHash == result.Hash
	e.mu.Unlock()
	if active {
		return result, fmt.Errorf("%w: torrent operation is active", ErrConflict)
	}
	current, err := e.repository.get(ctx, result.Hash)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return result, err
	}
	if err == nil && (current.State == StateTransferring || current.State == StateVerifying || current.CacheOperation != "" || current.CacheState == CacheEvicting || current.CacheState == CacheRestoring) {
		return result, fmt.Errorf("%w: torrent operation is active or requires recovery", ErrConflict)
	}
	torrent, manifest, files, err := e.fetchLifecyclePayload(ctx, result.Hash)
	if err != nil {
		return result, err
	}
	if !adoptionStateAllowed(torrent.State) {
		return result, &BlockedError{Reason: "stop the torrent and wait for metadata and storage operations to finish before adoption"}
	}
	if torrent.Size < 0 {
		return result, &BlockedError{Reason: "qBittorrent selected size is invalid"}
	}
	if _, err := NewRunner(e.source, e.destination).mappedGroups(manifest); err != nil {
		return result, err
	}
	if current.Hash != "" && (current.State == StateAdopted || current.VerifiedAt != nil || current.State == StateReplicated) {
		if e.identityMismatch(current) || e.requireManifest(current, manifest) != nil {
			return result, fmt.Errorf("%w: stored manifest or storage identity changed", ErrConflict)
		}
	}
	if err := e.runner.DestinationAvailable(ctx, manifest); err != nil {
		return result, &remoteAdoptionError{err}
	}
	cacheState, err := adoptionCacheState(ctx, e.source.LocalBasePath, manifest, files)
	if err != nil {
		return result, err
	}
	if cacheState == CacheEvicted {
		stopped, err := e.client.Stopped(ctx, result.Hash)
		if err != nil {
			return result, err
		}
		if !stopped {
			return result, &BlockedError{Reason: "stop the remote-only torrent in qBittorrent before adoption"}
		}
	}
	finalTorrent, finalManifest, finalFiles, err := e.fetchLifecyclePayload(ctx, result.Hash)
	if err != nil {
		return result, err
	}
	if !manifest.Equal(finalManifest) || !adoptionStateAllowed(finalTorrent.State) {
		return result, &BlockedError{Reason: "torrent metadata changed during adoption"}
	}
	finalCache, err := adoptionCacheState(ctx, e.source.LocalBasePath, manifest, finalFiles)
	if err != nil {
		return result, err
	}
	if cacheState != finalCache {
		return result, &BlockedError{Reason: "local payload changed during adoption"}
	}
	if cacheState == CacheEvicted {
		stopped, err := e.client.Stopped(ctx, result.Hash)
		if err != nil {
			return result, err
		}
		if !stopped {
			return result, &BlockedError{Reason: "torrent was started during adoption"}
		}
	}
	if current.Hash != "" && (current.VerifiedAt != nil || current.State == StateReplicated) {
		if err := requireVerifiedRecord(current, e); err != nil {
			return result, err
		}
		if current.CacheState != cacheState {
			return result, fmt.Errorf("%w: local payload does not match the registered cache state; adoption cannot replace a verified record", ErrConflict)
		}
		result.Status = "already_verified"
		result.Record = &current
		return result, nil
	}
	if current.State == StateAdopted {
		if current.VerifiedAt != nil || current.CacheState != cacheState {
			return result, fmt.Errorf("%w: adopted cache state changed; use restore to recover payloads", ErrConflict)
		}
		result.Status = "already_adopted"
		result.Record = &current
		return result, nil
	}
	adopted := e.newRecord(result.Hash, StateAdopted, manifest)
	adopted.CacheState = cacheState
	if current.Hash != "" {
		adopted.CreatedAt = current.CreatedAt
		adopted.Attempts = current.Attempts
	}
	result.Record = &adopted
	if dryRun {
		result.Status = "would_adopt"
		return result, nil
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := e.repository.adopt(ctx, current, adopted); err != nil {
		return result, err
	}
	result.Status = "adopted"
	e.log.Info().Str("hash", result.Hash).Str("cache_state", string(cacheState)).Msg("remote payload adopted without content verification")
	e.signal()
	return result, nil
}

func adoptionCacheState(ctx context.Context, root string, manifest Manifest, files []qbittorrent.File) (CacheState, error) {
	selected := make(map[int]qbittorrent.File)
	for _, file := range files {
		if file.Priority > 0 {
			selected[file.Index] = file
		}
	}

	if _, err := lstatPathComponents(root); err != nil {
		return "", err
	}
	present := 0
	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		info, exists, err := lstatRelativeNoSymlinks(root, filepath.FromSlash(file.Path))
		if err != nil {
			return "", err
		}

		parent := filepath.Join(root, filepath.Dir(filepath.FromSlash(file.Path)))
		if err := validatePartialPath(parent, filepath.Base(file.Path), file.Path); err != nil {
			return "", err
		}
		partialFile := filepath.Join(parent, partialDirectory, filepath.Base(file.Path))
		if _, err := os.Lstat(partialFile); err == nil {
			return "", &BlockedError{Reason: "local payload contains incomplete transfer data; adoption leaves it untouched"}
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		_, incomplete, err := lstatRelativeNoSymlinks(root, filepath.FromSlash(file.Path+".!qB"))
		if err != nil {
			return "", err
		}
		if incomplete {
			return "", &BlockedError{Reason: "local payload contains incomplete qBittorrent data; adoption leaves it untouched"}
		}

		if !exists {
			continue
		}
		if !info.Mode().IsRegular() || info.Size() != file.Size {
			return "", &BlockedError{Reason: fmt.Sprintf("local payload %q is unsafe or has an unexpected size", file.Path)}
		}
		progress, ok := selected[file.Index]
		if !ok || progress.Size != file.Size || !completeFileProgress(progress.Progress, file.Size) {
			return "", &BlockedError{Reason: fmt.Sprintf("qBittorrent reports local payload %q as incomplete", file.Path)}
		}
		present++
	}
	if present == 0 {
		return CacheEvicted, nil
	}
	if present == len(manifest.Files) {
		return CachePresent, nil
	}
	return "", &BlockedError{Reason: "local payload is partially present; adoption leaves partial files untouched"}
}

func (e *Engine) AdoptAll(ctx context.Context, dryRun bool) (AdoptionReport, error) {
	report := AdoptionReport{DryRun: dryRun, Results: make([]AdoptionResult, 0)}
	torrents, reservation, err := e.beginBulkAdoption(ctx)
	if err != nil {
		return report, err
	}
	defer e.releaseAdoptions(reservation)
	sort.Slice(torrents, func(i, j int) bool { return torrents[i].Hash < torrents[j].Hash })
	seen := make(map[string]bool, len(torrents))
	for _, torrent := range torrents {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		hash := strings.ToLower(strings.TrimSpace(torrent.Hash))
		if seen[hash] {
			continue
		}
		seen[hash] = true
		result, err := e.adoptReserved(ctx, hash, dryRun, reservation)
		if err != nil {
			if ctx.Err() != nil {
				return report, ctx.Err()
			}
			result.Error = err.Error()
			result.Record = nil
			var remote *remoteAdoptionError
			var missing *RemotePayloadError
			switch {
			case errors.As(err, &remote):
				if errors.As(remote.error, &missing) {
					result.Status = "missing_remote"
				} else if isBlocked(remote.error) {
					result.Status = "blocked"
				} else {
					result.Status = "error"
				}
			case errors.Is(err, ErrConflict), isBlocked(err), errors.Is(err, ErrIneligible):
				result.Status = "blocked"
			default:
				result.Status = "error"
			}
		}
		report.Results = append(report.Results, result)
		switch result.Status {
		case "adopted":
			report.Summary.Adopted++
		case "would_adopt":
			report.Summary.WouldAdopt++
		case "already_adopted":
			report.Summary.AlreadyAdopted++
		case "already_verified":
			report.Summary.AlreadyVerified++
		case "missing_remote":
			report.Summary.MissingRemote++
		case "blocked":
			report.Summary.Blocked++
		default:
			report.Summary.Errors++
		}
	}
	return report, nil
}

func (e *Engine) acquireCatalog(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case e.catalog <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (e *Engine) releaseCatalog() { <-e.catalog }

func adoptionStateAllowed(state string) bool {
	return readyTorrentState(state) || state == "missingFiles" || state == "stoppedDL" || state == "pausedDL"
}
