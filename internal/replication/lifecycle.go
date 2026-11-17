package replication

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/anish-chanda/seedcache/internal/config"
	"github.com/anish-chanda/seedcache/internal/qbittorrent"
	"golang.org/x/sys/unix"
)

const seedStateWaitLimit = 30 * time.Second

func (e *Engine) Evict(ctx context.Context, hash string) (Record, error) {
	if err := e.acquireCatalog(ctx); err != nil {
		return Record{}, err
	}
	defer e.releaseCatalog()
	normalized, err := normalizeHash(hash)
	if err != nil {
		return Record{}, err
	}
	if !validLifecycleHash(normalized) {
		return Record{}, fmt.Errorf("%w: torrent hash must be 40 or 64 hexadecimal characters", ErrConflict)
	}
	if e.adoptionReservations[normalized] > 0 {
		return Record{}, fmt.Errorf("%w: torrent is reserved for adoption", ErrConflict)
	}
	record, err := e.repository.get(ctx, normalized)
	if err != nil {
		return Record{}, err
	}
	if err := requireVerifiedRecord(record, e); err != nil {
		return record, err
	}
	e.mu.Lock()
	active := e.activeHash == normalized
	e.mu.Unlock()
	if active {
		return record, fmt.Errorf("%w: torrent replication is active", ErrConflict)
	}
	if record.CacheOperation == CacheOperationEvict &&
		(record.CacheState == CachePresent || record.CacheState == CacheEvicting) {
		if record.CacheError == "" {
			return record, nil
		}
		updated := record
		updated.CacheError = ""
		updated.CacheUpdatedAt = time.Now().UTC()
		return e.persistQueuedCacheOperation(ctx, record, updated, "eviction retry requested")
	}
	if record.CacheState != CachePresent || record.CacheOperation != "" {
		return record, fmt.Errorf("%w: torrent cache lifecycle is %s", ErrConflict, cacheDescription(record))
	}
	updated := record
	updated.CacheOperation = CacheOperationEvict
	updated.CacheError = ""
	updated.CacheUpdatedAt = time.Now().UTC()
	return e.persistQueuedCacheOperation(ctx, record, updated, "eviction requested")
}

func (e *Engine) Restore(ctx context.Context, hash string) (Record, error) {
	if err := e.acquireCatalog(ctx); err != nil {
		return Record{}, err
	}
	defer e.releaseCatalog()
	normalized, err := normalizeHash(hash)
	if err != nil {
		return Record{}, err
	}
	if !validLifecycleHash(normalized) {
		return Record{}, fmt.Errorf("%w: torrent hash must be 40 or 64 hexadecimal characters", ErrConflict)
	}
	if e.adoptionReservations[normalized] > 0 {
		return Record{}, fmt.Errorf("%w: torrent is reserved for adoption", ErrConflict)
	}
	record, err := e.repository.get(ctx, normalized)
	if err != nil {
		return Record{}, err
	}
	if err := requireRestorableRecord(record, e); err != nil {
		return record, err
	}
	e.mu.Lock()
	active := e.activeHash == normalized
	e.mu.Unlock()
	if active {
		return record, fmt.Errorf("%w: torrent replication is active", ErrConflict)
	}
	if record.CacheState == CachePresent && record.CacheOperation == CacheOperationSeed {
		if record.CacheError == "" {
			return record, nil
		}
		updated := record
		updated.CacheError = ""
		updated.CacheUpdatedAt = time.Now().UTC()
		return e.persistQueuedCacheOperation(ctx, record, updated, "seeding retry requested")
	}
	if record.CacheOperation == CacheOperationRestore &&
		(record.CacheState == CacheEvicted || record.CacheState == CacheRestoring || (record.State == StateAdopted && record.CacheState == CachePresent)) {
		if record.CacheError == "" {
			return record, nil
		}
		updated := record
		updated.CacheError = ""
		updated.CacheUpdatedAt = time.Now().UTC()
		return e.persistQueuedCacheOperation(ctx, record, updated, "restoration retry requested")
	}
	if (record.CacheState != CacheEvicted && !(record.State == StateAdopted && record.CacheState == CachePresent)) || record.CacheOperation != "" {
		return record, fmt.Errorf("%w: torrent cache state is %s; only evicted or adopted torrents can be restored", ErrConflict, cacheDescription(record))
	}
	updated := record
	updated.CacheOperation = CacheOperationRestore
	updated.CacheError = ""
	updated.CacheUpdatedAt = time.Now().UTC()
	return e.persistQueuedCacheOperation(ctx, record, updated, "restoration requested")
}

func (e *Engine) persistQueuedCacheOperation(ctx context.Context, expected, updated Record, message string) (Record, error) {
	changed, err := e.repository.replaceCache(ctx, expected, updated)
	if err != nil {
		return Record{}, err
	}
	if !changed {
		fresh, getErr := e.repository.get(ctx, expected.Hash)
		if getErr != nil {
			return Record{}, getErr
		}
		return fresh, fmt.Errorf("%w: cache lifecycle changed while the operation was being requested", ErrConflict)
	}
	e.log.Info().Str("hash", updated.Hash).Str("cache_operation", string(updated.CacheOperation)).Msg(message)
	e.signal()
	return e.repository.get(ctx, updated.Hash)
}

func requireVerifiedRecord(record Record, e *Engine) error {
	if record.State != StateReplicated || record.VerifiedAt == nil ||
		record.ManifestFingerprint == "" || record.ManifestFingerprint != record.Manifest.Fingerprint() {
		return fmt.Errorf("%w: torrent has no successful persisted verification record", ErrConflict)
	}
	if record.SourceIdentity != e.sourceIdentity() || record.DestinationIdentity != e.destinationIdentity() {
		return fmt.Errorf("%w: replication source or destination configuration changed", ErrConflict)
	}
	return nil
}

func validLifecycleHash(hash string) bool {
	if len(hash) != 40 && len(hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}

func cacheDescription(record Record) string {
	if record.CacheOperation != "" {
		if record.CacheError != "" {
			return fmt.Sprintf("%s (%s failed: %s)", record.CacheState, record.CacheOperation, record.CacheError)
		}
		return fmt.Sprintf("%s (%s queued)", record.CacheState, record.CacheOperation)
	}
	return string(record.CacheState)
}

func (e *Engine) runCacheOperation(ctx context.Context, record Record) error {
	e.mu.Lock()
	e.activeCacheHash = record.Hash
	e.activeCacheOperation = record.CacheOperation
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		if e.activeCacheHash == record.Hash {
			e.activeCacheHash = ""
			e.activeCacheOperation = ""
		}
		e.mu.Unlock()
	}()
	var err error
	switch record.CacheOperation {
	case CacheOperationEvict:
		err = e.runEviction(ctx, record)
	case CacheOperationRestore:
		err = e.runRestoration(ctx, record)
	case CacheOperationSeed:
		err = e.runResumeSeeding(ctx, record)
	default:
		err = fmt.Errorf("unknown cache operation %q", record.CacheOperation)
	}
	if err == nil {
		return nil
	}
	if persistErr := e.recordCacheError(record, err); persistErr != nil {
		return errors.Join(err, persistErr)
	}
	return nil
}

func (e *Engine) recordCacheError(record Record, cause error) error {
	ctx, cancel := persistenceContext()
	defer cancel()
	current, err := e.repository.get(ctx, record.Hash)
	if err != nil {
		return err
	}
	if current.CacheOperation != record.CacheOperation || current.CacheOperation == "" {
		return nil
	}
	updated := current
	updated.CacheError = cause.Error()
	updated.CacheUpdatedAt = time.Now().UTC()
	changed, err := e.repository.replaceCache(ctx, current, updated)
	if err != nil {
		return err
	}
	if changed {
		e.log.Warn().Err(cause).Str("hash", current.Hash).Str("cache_operation", string(current.CacheOperation)).Str("cache_state", string(current.CacheState)).Msg("cache lifecycle operation blocked")
		e.signal()
	}
	return nil
}

func (e *Engine) runEviction(ctx context.Context, record Record) error {
	if err := requireVerifiedRecord(record, e); err != nil {
		return err
	}
	if record.CacheOperation != CacheOperationEvict ||
		(record.CacheState != CachePresent && record.CacheState != CacheEvicting) {
		return fmt.Errorf("%w: eviction operation has an unexpected cache state", ErrConflict)
	}
	recovering := record.CacheState == CacheEvicting
	if !recovering {
		if err := e.waitForTorrentNotChecking(ctx, record.Hash); err != nil {
			return err
		}
	}
	if recovering {
		_, manifest, err := e.fetchLifecycleManifest(ctx, record.Hash)
		if err != nil {
			return err
		}
		if err := e.requireManifest(record, manifest); err != nil {
			return err
		}
		if err := validateManifestFiles(e.source, record.Manifest, true); err != nil {
			return err
		}
	} else {
		_, manifest, err := e.fetchManifest(ctx, record.Hash)
		if err != nil {
			return err
		}
		if err := e.requireManifest(record, manifest); err != nil {
			return err
		}
	}
	if err := e.runner.DestinationAvailable(ctx, record.Manifest); err != nil {
		return fmt.Errorf("destination payload is unavailable: %w", err)
	}
	if err := e.checkNoSharedPayload(ctx, record.Hash, record.Manifest, recovering); err != nil {
		return err
	}
	if err := e.client.Stop(ctx, record.Hash); err != nil {
		return fmt.Errorf("stop qBittorrent torrent: %w", err)
	}
	e.log.Info().Str("hash", record.Hash).Msg("torrent stopped for eviction")
	var currentManifest Manifest
	var err error
	if recovering {
		_, currentManifest, err = e.fetchLifecycleManifest(ctx, record.Hash)
	} else {
		_, currentManifest, err = e.fetchManifest(ctx, record.Hash)
	}
	if err != nil {
		return fmt.Errorf("revalidate torrent after stopping: %w", err)
	}
	if err := e.requireManifest(record, currentManifest); err != nil {
		return err
	}
	if err := validateManifestFiles(e.source, record.Manifest, recovering); err != nil {
		return err
	}
	if err := e.runner.DestinationAvailable(ctx, record.Manifest); err != nil {
		return fmt.Errorf("destination payload is unavailable after stopping: %w", err)
	}
	if err := e.checkNoSharedPayload(ctx, record.Hash, record.Manifest, recovering); err != nil {
		return err
	}
	if err := e.requireTorrentStopped(ctx, record.Hash); err != nil {
		return err
	}
	if !recovering {
		if _, err := e.updateCache(record, func(updated *Record) {
			updated.CacheState = CacheEvicting
			updated.CacheUpdatedAt = time.Now().UTC()
			updated.CacheError = ""
		}); err != nil {
			return err
		}
	}
	if err := removeManifestFiles(e.source, record.Manifest, recovering); err != nil {
		return err
	}
	updated, err := e.updateCacheCurrent(record.Hash, func(updated *Record) {
		updated.CacheState = CacheEvicted
		updated.CacheOperation = ""
		updated.CacheError = ""
		updated.CacheUpdatedAt = time.Now().UTC()
		updated.EvictedAt = ptrTime(updated.CacheUpdatedAt)
	})
	if err != nil {
		return err
	}
	e.log.Info().Str("hash", record.Hash).Str("cache_state", string(updated.CacheState)).Int64("total_size", record.Manifest.TotalSize).Msg("source payload removed")
	return nil
}

func (e *Engine) runRestoration(ctx context.Context, record Record) error {
	if err := requireRestorableRecord(record, e); err != nil {
		return err
	}
	if record.CacheOperation != CacheOperationRestore ||
		(record.CacheState != CacheEvicted && record.CacheState != CacheRestoring && !(record.State == StateAdopted && record.CacheState == CachePresent)) {
		return fmt.Errorf("%w: restoration operation has an unexpected cache state", ErrConflict)
	}
	recovering := record.CacheState == CacheRestoring
	torrent, manifest, err := e.fetchLifecycleManifest(ctx, record.Hash)
	if err != nil {
		return err
	}
	if err := e.requireManifest(record, manifest); err != nil {
		return err
	}
	if err := e.runner.DestinationAvailable(ctx, record.Manifest); err != nil {
		return fmt.Errorf("destination payload is unavailable: %w", err)
	}
	if err := requireSourceSpace(e.source, record.Manifest); err != nil {
		return err
	}
	if err := validateManifestFiles(e.source, record.Manifest, true); err != nil {
		return err
	}
	if err := e.checkNoSharedPayload(ctx, record.Hash, record.Manifest, true); err != nil {
		return err
	}
	if err := e.client.Stop(ctx, record.Hash); err != nil {
		return fmt.Errorf("ensure torrent is stopped before restoration: %w", err)
	}
	_, currentManifest, err := e.fetchLifecycleManifest(ctx, record.Hash)
	if err != nil {
		return fmt.Errorf("revalidate torrent after stopping: %w", err)
	}
	if err := e.requireManifest(record, currentManifest); err != nil {
		return err
	}
	if err := requireSourceSpace(e.source, record.Manifest); err != nil {
		return err
	}
	if err := e.checkNoSharedPayload(ctx, record.Hash, record.Manifest, true); err != nil {
		return err
	}
	if err := e.requireTorrentStopped(ctx, record.Hash); err != nil {
		return err
	}
	if !recovering {
		if _, err := e.updateCache(record, func(updated *Record) {
			updated.CacheState = CacheRestoring
			updated.CacheUpdatedAt = time.Now().UTC()
			updated.CacheError = ""
		}); err != nil {
			return err
		}
	}
	e.log.Info().Str("hash", record.Hash).Msg("restoration started")
	if err := e.runner.Restore(ctx, record.Manifest); err != nil {
		return fmt.Errorf("restore payload from destination: %w", err)
	}
	if err := validateManifestFiles(e.source, record.Manifest, false); err != nil {
		return fmt.Errorf("restored source payload is invalid: %w", err)
	}
	if err := e.runner.VerifyRestored(ctx, record.Manifest); err != nil {
		return fmt.Errorf("verify restored payload: %w", err)
	}
	// Stage an absent part-file away from the live path. An administrator
	// must install it while qBittorrent is stopped, then restart qBittorrent
	// so libtorrent can rebuild its cached part-file piece map.
	if partFiles, ok := e.runner.(interface {
		StagePartFile(context.Context, string, string, Manifest) (bool, error)
	}); ok {
		staged, err := partFiles.StagePartFile(ctx, record.Hash, torrent.SavePath, record.Manifest)
		if err != nil {
			return fmt.Errorf("restore qBittorrent part-file: %w", err)
		}
		if staged {
			return &BlockedError{Reason: "qBittorrent part-file staged separately; stop qBittorrent, install the staged .parts file in its save directory, restart qBittorrent, then retry restoration"}
		}
	}
	e.log.Info().Str("hash", record.Hash).Msg("restoration verified")
	e.cacheGuardMu.Lock()
	e.mu.Lock()
	e.activeCacheChecking = true
	e.mu.Unlock()
	e.cacheGuardMu.Unlock()
	defer e.endCacheChecking()
	recheckErr := e.client.Recheck(ctx, record.Hash)
	if recheckErr != nil {
		return fmt.Errorf("qBittorrent recheck failed: %w", recheckErr)
	}
	_, finalManifest, err := e.fetchManifest(ctx, record.Hash)
	if err != nil {
		return fmt.Errorf("confirm qBittorrent selected files after recheck: %w", err)
	}
	if err := e.requireManifest(record, finalManifest); err != nil {
		return err
	}
	e.log.Info().Str("hash", record.Hash).Msg("torrent recheck completed")
	persistCtx, persistCancel := persistenceContext()
	defer persistCancel()
	err = e.repository.completeRestoration(persistCtx, record.Hash, time.Now().UTC())
	e.signal()
	return err
}

func (e *Engine) endCacheChecking() {
	e.cacheGuardMu.Lock()
	e.mu.Lock()
	e.activeCacheChecking = false
	e.mu.Unlock()
	e.cacheGuardMu.Unlock()
}

func (e *Engine) runResumeSeeding(ctx context.Context, record Record) error {
	if err := requireVerifiedRecord(record, e); err != nil {
		return err
	}
	if record.CacheState != CachePresent || record.CacheOperation != CacheOperationSeed {
		return fmt.Errorf("%w: seeding resume has an unexpected cache state", ErrConflict)
	}
	if err := e.waitForTorrentNotChecking(ctx, record.Hash); err != nil {
		return err
	}
	_, manifest, err := e.fetchManifest(ctx, record.Hash)
	if err != nil {
		return err
	}
	if err := e.requireManifest(record, manifest); err != nil {
		return err
	}
	if err := e.client.Start(ctx, record.Hash); err != nil {
		return fmt.Errorf("resume qBittorrent seeding: %w", err)
	}
	updated, err := e.updateCacheCurrent(record.Hash, func(updated *Record) {
		updated.CacheOperation = ""
		updated.CacheError = ""
		updated.CacheUpdatedAt = time.Now().UTC()
	})
	if err != nil {
		return err
	}
	e.log.Info().Str("hash", record.Hash).Str("cache_state", string(updated.CacheState)).Msg("seeding resumed")
	return nil
}

func (e *Engine) waitForTorrentNotChecking(ctx context.Context, hash string) error {
	waitCtx, cancel := context.WithTimeout(ctx, seedStateWaitLimit)
	defer cancel()
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	for {
		torrent, err := e.client.Torrent(waitCtx, hash)
		if err != nil {
			return fmt.Errorf("check qBittorrent state before lifecycle operation: %w", err)
		}
		state := strings.ToLower(strings.TrimSpace(torrent.State))
		if !strings.Contains(state, "checking") {
			return nil
		}
		select {
		case <-waitCtx.Done():
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
				return fmt.Errorf("qBittorrent torrent %s remained in %q for %s", hash, torrent.State, seedStateWaitLimit)
			}
			return waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func (e *Engine) requireManifest(record Record, manifest Manifest) error {
	if !record.Manifest.Equal(manifest) || record.ManifestFingerprint != manifest.Fingerprint() {
		return &BlockedError{Reason: "qBittorrent selected-file manifest changed since verification"}
	}
	return nil
}

func (e *Engine) requireTorrentStopped(ctx context.Context, hash string) error {
	stopped, err := e.client.Stopped(ctx, hash)
	if err != nil {
		return fmt.Errorf("confirm qBittorrent torrent is stopped: %w", err)
	}
	if !stopped {
		return &BlockedError{Reason: "qBittorrent torrent is no longer stopped"}
	}
	return nil
}

func (e *Engine) fetchLifecycleManifest(ctx context.Context, hash string) (qbittorrent.Torrent, Manifest, error) {
	torrent, manifest, _, err := e.fetchLifecyclePayload(ctx, hash)
	return torrent, manifest, err
}

func (e *Engine) fetchLifecyclePayload(ctx context.Context, hash string) (qbittorrent.Torrent, Manifest, []qbittorrent.File, error) {
	torrent, err := e.client.Torrent(ctx, hash)
	if err != nil {
		if errors.Is(err, qbittorrent.ErrNotFound) {
			return qbittorrent.Torrent{}, Manifest{}, nil, fmt.Errorf("%w: torrent not found in qBittorrent", ErrNotFound)
		}
		if !errors.Is(err, context.Canceled) {
			e.Unavailable()
		}
		return qbittorrent.Torrent{}, Manifest{}, nil, fmt.Errorf("qBittorrent torrent lookup: %w", err)
	}
	files, err := e.client.Files(ctx, hash)
	if err != nil {
		if errors.Is(err, qbittorrent.ErrNotFound) {
			return torrent, Manifest{}, nil, fmt.Errorf("%w: torrent files not found in qBittorrent", ErrNotFound)
		}
		if !errors.Is(err, context.Canceled) {
			e.Unavailable()
		}
		return torrent, Manifest{}, nil, fmt.Errorf("qBittorrent file list: %w", err)
	}
	manifest, err := manifestFromTorrentMetadata(torrent, files, e.source)
	if err != nil {
		return torrent, Manifest{}, nil, err
	}
	return torrent, manifest, files, nil
}

func manifestFromTorrentMetadata(torrent qbittorrent.Torrent, files []qbittorrent.File, source config.SourceConfig) (Manifest, error) {
	if torrent.Size < 0 || torrent.TotalSize < 0 {
		return Manifest{}, &BlockedError{Reason: "qBittorrent returned invalid torrent sizes"}
	}
	if torrent.AutoTMM {
		return Manifest{}, &BlockedError{Reason: "automatic torrent management is enabled"}
	}
	if len(files) == 0 {
		return Manifest{}, &BlockedError{Reason: "qBittorrent returned no torrent files"}
	}
	qbBase, _, err := sourceRoots(source)
	if err != nil {
		return Manifest{}, &BlockedError{Reason: err.Error()}
	}
	savePath, err := cleanAbsolutePath(torrent.SavePath, "qBittorrent save_path")
	if err != nil {
		return Manifest{}, &BlockedError{Reason: err.Error()}
	}
	contentPath, err := cleanAbsolutePath(torrent.ContentPath, "qBittorrent content_path")
	if err != nil {
		return Manifest{}, &BlockedError{Reason: err.Error()}
	}
	if _, ok := relativeToRoot(qbBase, savePath); !ok {
		return Manifest{}, &BlockedError{Reason: "qBittorrent save_path is outside the configured source root"}
	}
	if _, ok := relativeToRoot(qbBase, contentPath); !ok {
		return Manifest{}, &BlockedError{Reason: "qBittorrent content_path is outside the configured source root"}
	}
	manifest := Manifest{Files: make([]File, 0, len(files))}
	allFilesSize := int64(0)
	seenIndexes := make(map[int]struct{}, len(files))
	seenPaths := make(map[string]struct{}, len(files))
	for _, file := range files {
		if file.Priority < 0 || file.Size < 0 || file.Index < 0 || !isFinite(file.Progress) || file.Progress < 0 || file.Progress > 1 {
			return Manifest{}, &BlockedError{Reason: fmt.Sprintf("qBittorrent returned invalid file metadata for %q", file.Name)}
		}
		if file.Size > math.MaxInt64-allFilesSize {
			return Manifest{}, &BlockedError{Reason: "torrent files exceed the supported total size"}
		}
		allFilesSize += file.Size
		if _, exists := seenIndexes[file.Index]; exists {
			return Manifest{}, &BlockedError{Reason: fmt.Sprintf("qBittorrent returned duplicate file index %d", file.Index)}
		}
		seenIndexes[file.Index] = struct{}{}
		if file.Priority == 0 {
			continue
		}
		name, err := cleanRelativePath(file.Name, "qBittorrent file name")
		if err != nil {
			return Manifest{}, &BlockedError{Reason: err.Error()}
		}
		qBPath := filepath.Clean(filepath.Join(savePath, name))
		qBRelative, ok := relativeToRoot(qbBase, qBPath)
		if !ok {
			return Manifest{}, &BlockedError{Reason: fmt.Sprintf("selected file %q is outside the configured source root", file.Name)}
		}
		if len(files) == 1 {
			if qBPath != contentPath {
				return Manifest{}, &BlockedError{Reason: "qBittorrent content_path does not match the single-file path"}
			}
		} else if _, ok := relativeToRoot(contentPath, qBPath); !ok || qBPath == contentPath {
			return Manifest{}, &BlockedError{Reason: fmt.Sprintf("selected file %q is outside qBittorrent content_path", file.Name)}
		}
		manifestPath := filepath.ToSlash(qBRelative)
		if _, exists := seenPaths[manifestPath]; exists {
			return Manifest{}, &BlockedError{Reason: fmt.Sprintf("selected torrent files collide at %q", manifestPath)}
		}
		seenPaths[manifestPath] = struct{}{}
		if file.Size > math.MaxInt64-manifest.TotalSize {
			return Manifest{}, &BlockedError{Reason: "selected torrent files exceed the supported total size"}
		}
		manifest.Files = append(manifest.Files, File{Path: manifestPath, Size: file.Size, Index: file.Index})
		manifest.TotalSize += file.Size
	}
	if len(manifest.Files) == 0 {
		return Manifest{}, &BlockedError{Reason: "torrent has no selected files"}
	}
	if torrent.Size > 0 && !consistentSelectedSize(torrent, manifest.TotalSize, allFilesSize, manifest.TotalSize < allFilesSize) {
		return Manifest{}, &BlockedError{Reason: "qBittorrent selected size does not match its file list"}
	}
	sort.Slice(manifest.Files, func(i, j int) bool {
		if manifest.Files[i].Path != manifest.Files[j].Path {
			return manifest.Files[i].Path < manifest.Files[j].Path
		}
		return manifest.Files[i].Index < manifest.Files[j].Index
	})
	if _, _, err := validateManifest(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func validateManifestFiles(source config.SourceConfig, manifest Manifest, allowMissing bool) error {
	files, _, err := validateManifest(manifest)
	if err != nil {
		return err
	}
	_, root, err := sourceRoots(source)
	if err != nil {
		return &BlockedError{Reason: err.Error()}
	}
	if !allowMissing {
		return validateLocalFiles(root, files)
	}
	if _, err := lstatPathComponents(root); err != nil {
		return err
	}
	for _, file := range files {
		info, exists, err := lstatRelativeNoSymlinks(root, filepath.FromSlash(file.Path))
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		if !info.Mode().IsRegular() {
			return &BlockedError{Reason: fmt.Sprintf("source payload %q is not a regular file", file.Path)}
		}
		if info.Size() != file.Size {
			return &BlockedError{Reason: fmt.Sprintf("source payload %q has size %d, expected %d", file.Path, info.Size(), file.Size)}
		}
	}
	return nil
}

func lstatRelativeNoSymlinks(root, relative string) (os.FileInfo, bool, error) {
	clean, err := cleanRelativePath(relative, "source payload path")
	if err != nil {
		return nil, false, &BlockedError{Reason: err.Error()}
	}
	current := root
	parts := strings.Split(clean, string(filepath.Separator))
	for index, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, fmt.Errorf("inspect source payload %q: %w", filepath.ToSlash(clean), err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, false, &BlockedError{Reason: fmt.Sprintf("source payload %q traverses a symlink", filepath.ToSlash(clean))}
		}
		if index < len(parts)-1 && !info.IsDir() {
			return nil, false, &BlockedError{Reason: fmt.Sprintf("source path component in %q is not a directory", filepath.ToSlash(clean))}
		}
		if index == len(parts)-1 {
			return info, true, nil
		}
	}
	return nil, false, nil
}

func removeManifestFiles(source config.SourceConfig, manifest Manifest, allowMissing bool) error {
	files, _, err := validateManifest(manifest)
	if err != nil {
		return err
	}
	_, root, err := sourceRoots(source)
	if err != nil {
		return &BlockedError{Reason: err.Error()}
	}
	rootFD, err := openAbsoluteDirectoryNoSymlinks(root)
	if err != nil {
		return fmt.Errorf("open source root for safe deletion: %w", err)
	}
	defer unix.Close(rootFD)
	for _, file := range files {
		if err := unlinkRelativeFile(rootFD, file, allowMissing); err != nil {
			return err
		}
	}
	for _, file := range files {
		_, exists, err := lstatRelativeNoSymlinks(root, filepath.FromSlash(file.Path))
		if err != nil {
			return err
		}
		if exists {
			return &BlockedError{Reason: fmt.Sprintf("source payload %q remains after eviction", file.Path)}
		}
	}
	return nil
}

func openAbsoluteDirectoryNoSymlinks(path string) (int, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return -1, errors.New("source root must be a clean absolute path")
	}
	fd, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	for _, component := range strings.Split(strings.TrimPrefix(path, string(filepath.Separator)), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return -1, openErr
		}
		fd = next
	}
	return fd, nil
}

func unlinkRelativeFile(rootFD int, file File, allowMissing bool) error {
	parts := strings.Split(filepath.FromSlash(file.Path), string(filepath.Separator))
	currentFD := rootFD
	opened := make([]int, 0, len(parts)-1)
	defer func() {
		for _, fd := range opened {
			_ = unix.Close(fd)
		}
	}()
	for _, component := range parts[:len(parts)-1] {
		fd, err := unix.Openat(currentFD, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if errors.Is(err, unix.ENOENT) && allowMissing {
			return nil
		}
		if err != nil {
			if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) {
				return &BlockedError{Reason: fmt.Sprintf("source path component in %q is unsafe", file.Path)}
			}
			return fmt.Errorf("open source path component for %q: %w", file.Path, err)
		}
		opened = append(opened, fd)
		currentFD = fd
	}
	leaf := parts[len(parts)-1]
	var stat unix.Stat_t
	err := unix.Fstatat(currentFD, leaf, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) && allowMissing {
		return nil
	}
	if err != nil {
		if errors.Is(err, unix.ELOOP) {
			return &BlockedError{Reason: fmt.Sprintf("source payload %q is a symlink", file.Path)}
		}
		return fmt.Errorf("inspect source payload %q before deletion: %w", file.Path, err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Size != file.Size {
		return &BlockedError{Reason: fmt.Sprintf("source payload %q changed before deletion", file.Path)}
	}
	if err := unix.Unlinkat(currentFD, leaf, 0); err != nil {
		return fmt.Errorf("remove source payload %q: %w", file.Path, err)
	}
	return nil
}

func (e *Engine) checkNoSharedPayload(ctx context.Context, hash string, manifest Manifest, allowMissing bool) error {
	qbBase, localRoot, err := sourceRoots(e.source)
	if err != nil {
		return &BlockedError{Reason: err.Error()}
	}
	if err := validateManifestFiles(e.source, manifest, allowMissing); err != nil {
		return err
	}
	targets := make(map[string]os.FileInfo, len(manifest.Files))
	for _, file := range manifest.Files {
		path := filepath.Join(localRoot, filepath.FromSlash(file.Path))
		info, exists, err := lstatRelativeNoSymlinks(localRoot, filepath.FromSlash(file.Path))
		if err != nil {
			return err
		}
		if exists {
			targets[path] = info
		} else {
			targets[path] = nil
		}
	}
	torrents, err := e.client.Torrents(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			e.Unavailable()
		}
		return fmt.Errorf("inspect qBittorrent torrent ownership: %w", err)
	}
	seenHashes := make(map[string]struct{}, len(torrents))
	for _, torrent := range torrents {
		otherHash, hashErr := normalizeHash(torrent.Hash)
		if hashErr != nil {
			return &BlockedError{Reason: "qBittorrent returned a torrent without an identifiable hash"}
		}
		if otherHash == hash {
			continue
		}
		if _, duplicate := seenHashes[otherHash]; duplicate {
			return &BlockedError{Reason: "qBittorrent returned duplicate torrent hashes; file ownership is ambiguous"}
		}
		seenHashes[otherHash] = struct{}{}
		mappedRoots := make([]string, 0, 2)
		for _, rawRoot := range []string{torrent.SavePath, torrent.DownloadPath} {
			if strings.TrimSpace(rawRoot) == "" {
				continue
			}
			mappedRoot, err := cleanAbsolutePath(rawRoot, "qBittorrent file root")
			if err != nil {
				return &BlockedError{Reason: fmt.Sprintf("cannot determine file ownership for torrent %s: %v", otherHash, err)}
			}
			mappedRoots = append(mappedRoots, mappedRoot)
		}
		contentPath, err := cleanAbsolutePath(torrent.ContentPath, "qBittorrent content_path")
		if err != nil {
			return &BlockedError{Reason: fmt.Sprintf("cannot determine file ownership for torrent %s: %v", otherHash, err)}
		}
		files, err := e.client.Files(ctx, otherHash)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				e.Unavailable()
			}
			return fmt.Errorf("inspect file ownership for torrent %s: %w", otherHash, err)
		}
		if len(files) == 0 {
			return &BlockedError{Reason: fmt.Sprintf("qBittorrent returned no file list for torrent %s; file ownership is ambiguous", otherHash)}
		}
		mappedAny := false
		for _, otherFile := range files {
			name, err := cleanRelativePath(otherFile.Name, "qBittorrent file name")
			if err != nil {
				return &BlockedError{Reason: fmt.Sprintf("cannot determine file ownership for torrent %s: %v", otherHash, err)}
			}
			for _, mappedRoot := range mappedRoots {
				qbPath := filepath.Clean(filepath.Join(mappedRoot, name))
				qbRelative, inside := relativeToRoot(qbBase, qbPath)
				if !inside {
					if _, rootInside := relativeToRoot(qbBase, mappedRoot); rootInside {
						return &BlockedError{Reason: fmt.Sprintf("torrent %s has a file outside the configured source root", otherHash)}
					}
					continue
				}
				mappedAny = true
				otherPath := filepath.Join(localRoot, qbRelative)
				otherInfo, exists, inspectErr := lstatRelativeNoSymlinks(localRoot, qbRelative)
				if inspectErr != nil {
					return inspectErr
				}
				for targetPath, targetInfo := range targets {
					if pathsOverlap(targetPath, otherPath) {
						return &BlockedError{Reason: fmt.Sprintf("source file %q is also referenced by torrent %s", filepath.Base(targetPath), otherHash)}
					}
					if exists && targetInfo != nil && os.SameFile(targetInfo, otherInfo) {
						return &BlockedError{Reason: fmt.Sprintf("source file %q is hard-linked to a file referenced by torrent %s", filepath.Base(targetPath), otherHash)}
					}
				}
			}
		}
		_, contentInside := relativeToRoot(qbBase, contentPath)
		if mappedAny != contentInside {
			return &BlockedError{Reason: fmt.Sprintf("torrent %s has an ambiguous content path or file mapping", otherHash)}
		}
		if !mappedAny {
			return &BlockedError{Reason: fmt.Sprintf("cannot establish source-file ownership for torrent %s outside the configured source mapping", otherHash)}
		}
	}
	return nil
}

func pathsOverlap(left, right string) bool {
	_, rightWithinLeft := relativeToRoot(left, right)
	_, leftWithinRight := relativeToRoot(right, left)
	return rightWithinLeft || leftWithinRight
}

func requireSourceSpace(source config.SourceConfig, manifest Manifest) error {
	files, _, err := validateManifest(manifest)
	if err != nil {
		return err
	}
	_, root, err := sourceRoots(source)
	if err != nil {
		return &BlockedError{Reason: err.Error()}
	}
	if _, err := lstatPathComponents(root); err != nil {
		return err
	}
	var bytesNeeded uint64
	for _, file := range files {
		info, exists, err := lstatRelativeNoSymlinks(root, filepath.FromSlash(file.Path))
		if err != nil {
			return err
		}
		if exists {
			if !info.Mode().IsRegular() || info.Size() != file.Size {
				return &BlockedError{Reason: fmt.Sprintf("source payload path %q already contains different data", file.Path)}
			}
			continue
		}
		if uint64(file.Size) > math.MaxUint64-bytesNeeded {
			return &BlockedError{Reason: "restoration manifest exceeds the supported total size"}
		}
		bytesNeeded += uint64(file.Size)
	}
	var stats unix.Statfs_t
	if err := unix.Statfs(root, &stats); err != nil {
		return fmt.Errorf("check free space on source storage: %w", err)
	}
	available := stats.Bavail * uint64(stats.Bsize)
	if available < bytesNeeded {
		return &BlockedError{Reason: fmt.Sprintf("source storage has %s free but restoration requires %s", humanBytes(available), humanBytes(bytesNeeded))}
	}
	return nil
}

func humanBytes(value uint64) string {
	const unit = 1024
	if value < unit {
		return fmt.Sprintf("%d B", value)
	}
	div, exp := uint64(unit), 0
	for n := value / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(value)/float64(div), "KMGTPE"[exp])
}

func (e *Engine) updateCache(expected Record, change func(*Record)) (Record, error) {
	e.cacheGuardMu.Lock()
	defer e.cacheGuardMu.Unlock()
	updated := expected
	change(&updated)
	ctx, cancel := persistenceContext()
	defer cancel()
	changed, err := e.repository.replaceCache(ctx, expected, updated)
	if err != nil {
		return Record{}, err
	}
	if !changed {
		return Record{}, fmt.Errorf("%w: cache lifecycle changed during the operation", ErrConflict)
	}
	e.signal()
	return e.repository.get(ctx, expected.Hash)
}

func (e *Engine) updateCacheCurrent(hash string, change func(*Record)) (Record, error) {
	e.cacheGuardMu.Lock()
	defer e.cacheGuardMu.Unlock()
	ctx, cancel := persistenceContext()
	defer cancel()
	current, err := e.repository.get(ctx, hash)
	if err != nil {
		return Record{}, err
	}
	updated := current
	change(&updated)
	changed, err := e.repository.replaceCache(ctx, current, updated)
	if err != nil {
		return Record{}, err
	}
	if !changed {
		return Record{}, fmt.Errorf("%w: cache lifecycle changed during the operation", ErrConflict)
	}
	e.signal()
	return e.repository.get(ctx, hash)
}

func (e *Engine) EvictionPlan(ctx context.Context) (EvictionPlan, error) {
	e.mu.Lock()
	retention := e.retention
	e.mu.Unlock()
	plan := EvictionPlan{
		Enabled:           retention.AutoEvict,
		LowFreePercent:    retention.LowFreePercent,
		TargetFreePercent: retention.TargetFreePercent,
		Candidates:        make([]EvictionCandidate, 0),
	}
	_, root, err := sourceRoots(e.source)
	if err != nil {
		return plan, err
	}
	if _, err := lstatPathComponents(root); err != nil {
		return plan, err
	}
	var stats unix.Statfs_t
	if err := unix.Statfs(root, &stats); err != nil {
		return plan, fmt.Errorf("read source filesystem free space: %w", err)
	}
	plan.FreeBytes = stats.Bavail * uint64(stats.Bsize)
	plan.TotalBytes = stats.Blocks * uint64(stats.Bsize)
	if plan.TotalBytes > 0 {
		plan.FreePercent = 100 * float64(plan.FreeBytes) / float64(plan.TotalBytes)
	}
	if plan.TotalBytes > 0 && retention.TargetFreePercent > 0 {
		target := math.Ceil(float64(plan.TotalBytes) * retention.TargetFreePercent / 100)
		if target > float64(plan.FreeBytes) && target < float64(math.MaxUint64) {
			plan.BytesToTarget = uint64(target) - plan.FreeBytes
		}
	}
	plan.Triggered = retention.AutoEvict && plan.FreePercent < retention.LowFreePercent
	torrents, err := e.client.Torrents(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			e.Unavailable()
		}
		return plan, fmt.Errorf("read torrents for retention planning: %w", err)
	}
	records, err := e.repository.list(ctx)
	if err != nil {
		return plan, err
	}
	byHash := make(map[string]qbittorrent.Torrent, len(torrents))
	for _, torrent := range torrents {
		if hash, err := normalizeHash(torrent.Hash); err == nil {
			byHash[hash] = torrent
		}
	}
	for _, record := range records {
		if record.State != StateReplicated || record.CacheState != CachePresent || record.CacheOperation != "" ||
			record.VerifiedAt == nil || record.ManifestFingerprint != record.Manifest.Fingerprint() ||
			e.identityMismatch(record) {
			continue
		}
		torrent, ok := byHash[record.Hash]
		minimumSeedingSeconds := int64(retention.MinSeedingTime / time.Second)
		if retention.MinSeedingTime%time.Second != 0 {
			minimumSeedingSeconds++
		}
		if !ok || torrent.AutoTMM || !isFinite(torrent.Ratio) || torrent.Ratio < 0 || torrent.Ratio < retention.MinRatio ||
			torrent.SeedingTime < minimumSeedingSeconds {
			continue
		}
		candidate := EvictionCandidate{
			Hash:        record.Hash,
			Name:        torrent.Name,
			Ratio:       torrent.Ratio,
			SeedingTime: time.Duration(torrent.SeedingTime) * time.Second,
			TotalSize:   record.Manifest.TotalSize,
		}
		if torrent.LastActivity > 0 {
			candidate.LastActivity = time.Unix(torrent.LastActivity, 0).UTC()
		}
		_, currentManifest, manifestErr := e.fetchManifest(ctx, record.Hash)
		if manifestErr != nil {
			if isBlocked(manifestErr) || errors.Is(manifestErr, ErrIneligible) || errors.Is(manifestErr, qbittorrent.ErrNotFound) {
				continue
			}
			return plan, manifestErr
		}
		if manifestErr = e.requireManifest(record, currentManifest); manifestErr != nil {
			continue
		}
		if manifestErr = e.runner.DestinationAvailable(ctx, record.Manifest); manifestErr != nil {
			if isBlocked(manifestErr) {
				continue
			}
			return plan, manifestErr
		}
		if manifestErr = e.checkNoSharedPayload(ctx, record.Hash, record.Manifest, false); manifestErr != nil {
			if isBlocked(manifestErr) {
				continue
			}
			return plan, manifestErr
		}
		plan.Candidates = append(plan.Candidates, candidate)
	}
	sort.Slice(plan.Candidates, func(i, j int) bool {
		left, right := plan.Candidates[i], plan.Candidates[j]
		if left.LastActivity.IsZero() != right.LastActivity.IsZero() {
			return !left.LastActivity.IsZero()
		}
		if !left.LastActivity.Equal(right.LastActivity) {
			return left.LastActivity.Before(right.LastActivity)
		}
		return left.Hash < right.Hash
	})
	if !retention.AutoEvict {
		plan.Message = "automatic eviction is disabled"
	} else if len(plan.Candidates) == 0 {
		if plan.Triggered {
			plan.Message = "no safe torrents meet the configured seeding requirements"
		} else {
			plan.Message = "source free space is above the low threshold"
		}
	}
	return plan, nil
}

func (e *Engine) queueAutomaticEviction(ctx context.Context) (bool, error) {
	e.mu.Lock()
	enabled := e.retention.AutoEvict
	e.mu.Unlock()
	if !enabled {
		e.mu.Lock()
		e.retentionActive = false
		e.retentionPressureLogged = false
		e.retentionExhaustedLogged = false
		e.mu.Unlock()
		return false, nil
	}
	plan, err := e.EvictionPlan(ctx)
	if err != nil {
		return false, err
	}
	e.mu.Lock()
	if !e.retentionActive && plan.Triggered {
		e.retentionActive = true
	}
	if e.retentionActive && plan.FreePercent >= plan.TargetFreePercent {
		e.retentionActive = false
		e.retentionPressureLogged = false
		e.retentionExhaustedLogged = false
		e.mu.Unlock()
		e.log.Info().Float64("free_percent", plan.FreePercent).Float64("target_free_percent", plan.TargetFreePercent).Msg("automatic eviction pressure cleared")
		return false, nil
	}
	active := e.retentionActive
	if active && !e.retentionPressureLogged {
		e.retentionPressureLogged = true
		e.mu.Unlock()
		e.log.Info().Float64("free_percent", plan.FreePercent).Float64("low_free_percent", plan.LowFreePercent).Float64("target_free_percent", plan.TargetFreePercent).Msg("automatic eviction triggered")
		e.mu.Lock()
	}
	if !active {
		e.mu.Unlock()
		return false, nil
	}
	if len(plan.Candidates) == 0 {
		e.retentionActive = false
		if !e.retentionExhaustedLogged {
			e.retentionExhaustedLogged = true
			e.mu.Unlock()
			e.log.Warn().Float64("free_percent", plan.FreePercent).Float64("target_free_percent", plan.TargetFreePercent).Msg("automatic eviction found no safe candidates")
		} else {
			e.mu.Unlock()
		}
		return false, nil
	}
	e.retentionExhaustedLogged = false
	e.mu.Unlock()
	if err := e.acquireCatalog(ctx); err != nil {
		return false, err
	}
	defer e.releaseCatalog()
	for _, candidate := range plan.Candidates {
		if e.adoptionReservations[candidate.Hash] > 0 {
			continue
		}
		record, err := e.repository.get(ctx, candidate.Hash)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return false, err
		}
		if record.CacheState != CachePresent || record.CacheOperation != "" || record.State != StateReplicated {
			continue
		}
		updated := record
		updated.CacheOperation = CacheOperationEvict
		updated.CacheError = ""
		updated.CacheUpdatedAt = time.Now().UTC()
		changed, err := e.repository.replaceCache(ctx, record, updated)
		if err != nil {
			return false, err
		}
		if !changed {
			continue
		}
		e.signal()
		return true, nil
	}
	return false, nil
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func (e *Engine) reconcileEvictedTorrents(ctx context.Context, torrents []qbittorrent.Torrent) {
	for _, torrent := range torrents {
		hash, err := normalizeHash(torrent.Hash)
		if err != nil {
			continue
		}
		e.cacheGuardMu.Lock()
		record, err := e.repository.get(ctx, hash)
		if err != nil || (record.CacheState != CacheEvicted && record.CacheState != CacheEvicting && record.CacheState != CacheRestoring) {
			e.cacheGuardMu.Unlock()
			continue
		}
		e.mu.Lock()
		checking := e.activeCacheHash == hash && e.activeCacheChecking
		e.mu.Unlock()
		if checking {
			e.cacheGuardMu.Unlock()
			continue
		}
		stopped, err := e.client.Stopped(ctx, hash)
		if err != nil {
			e.log.Warn().Err(err).Str("hash", hash).Msg("could not check whether an evicted torrent is stopped")
			e.cacheGuardMu.Unlock()
			continue
		}
		if stopped {
			e.cacheGuardMu.Unlock()
			continue
		}
		if err := e.client.Stop(ctx, hash); err != nil {
			e.log.Warn().Err(err).Str("hash", hash).Msg("could not stop unexpectedly started evicted torrent")
			e.cacheGuardMu.Unlock()
			continue
		}
		e.log.Warn().Str("hash", hash).Msg("stopped unexpectedly started evicted torrent")
		e.cacheGuardMu.Unlock()
	}
}

func requireRestorableRecord(record Record, e *Engine) error {
	if record.State == StateAdopted {
		if record.VerifiedAt != nil || record.ManifestFingerprint == "" || record.ManifestFingerprint != record.Manifest.Fingerprint() || e.identityMismatch(record) {
			return fmt.Errorf("%w: adopted manifest or storage identity changed", ErrConflict)
		}
		if _, _, err := validateManifest(record.Manifest); err != nil {
			return err
		}
		return nil
	}
	return requireVerifiedRecord(record, e)
}
