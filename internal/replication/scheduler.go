package replication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/anish-chanda/seedcache/internal/config"
	"github.com/anish-chanda/seedcache/internal/qbittorrent"
	"github.com/rs/zerolog"
)

const (
	baseRetryDelay  = 30 * time.Second
	maxRetryDelay   = 30 * time.Minute
	databaseTimeout = 5 * time.Second
)

type runner interface {
	Transfer(context.Context, Manifest) error
	Verify(context.Context, Manifest) error
}

type Engine struct {
	repository  *repository
	client      *qbittorrent.Client
	source      config.SourceConfig
	destination config.DestinationConfig
	runner      runner
	log         zerolog.Logger
	wake        chan struct{}

	mu           sync.Mutex
	connected    bool
	snapshot     map[string]qbittorrent.Torrent
	activeHash   string
	activeCancel context.CancelFunc
}

func NewEngine(db *sql.DB, client *qbittorrent.Client, source config.SourceConfig, destination config.DestinationConfig, log zerolog.Logger) (*Engine, error) {
	if db == nil {
		return nil, errors.New("replication database is required")
	}
	if client == nil {
		return nil, errors.New("qBittorrent client is required")
	}
	return &Engine{
		repository:  &repository{db: db},
		client:      client,
		source:      source,
		destination: destination,
		runner:      NewRunner(source, destination),
		log:         log,
		wake:        make(chan struct{}, 1),
		snapshot:    make(map[string]qbittorrent.Torrent),
	}, nil
}

func (e *Engine) List(ctx context.Context) ([]Record, error) {
	records, err := e.repository.list(ctx)
	if err != nil {
		return nil, fmt.Errorf("list replication records: %w", err)
	}
	return records, nil
}

func (e *Engine) Get(ctx context.Context, hash string) (Record, error) {
	normalized, err := normalizeHash(hash)
	if err != nil {
		return Record{}, err
	}
	record, err := e.repository.get(ctx, normalized)
	if err != nil {
		return Record{}, err
	}
	return record, nil
}

func (e *Engine) Summary(ctx context.Context) (Summary, error) {
	summary, err := e.repository.summary(ctx)
	if err != nil {
		return Summary{}, err
	}
	return summary, nil
}

// Queue captures torrent metadata and stores a pending record. Transfers are
// started only by Run, outside the caller's request.
func (e *Engine) Queue(ctx context.Context, hash string) (Record, error) {
	normalized, err := normalizeHash(hash)
	if err != nil {
		return Record{}, err
	}
	current, err := e.repository.get(ctx, normalized)
	if err == nil {
		switch current.State {
		case StatePending, StateTransferring, StateVerifying:
			return current, nil
		case StateReplicated:
			if !e.identityMismatch(current) {
				return current, nil
			}
			blocked, blockErr := e.blockReconfigured(current)
			if blockErr != nil {
				return Record{}, blockErr
			}
			return blocked, &BlockedError{Reason: blocked.LastError}
		case StateFailed, StateBlocked:
			return current, fmt.Errorf("%w: use retry for a %s replication", ErrConflict, current.State)
		}
	} else if !errors.Is(err, ErrNotFound) {
		return Record{}, err
	}

	torrent, manifest, err := e.fetchManifest(ctx, normalized)
	if err != nil {
		var blocked *BlockedError
		if errors.As(err, &blocked) {
			if persistErr := e.recordBlocked(normalized, blocked.Error()); persistErr != nil {
				return Record{}, errors.Join(err, persistErr)
			}
		}
		return Record{}, err
	}
	record := e.newRecord(torrent.Hash, StatePending, manifest)
	created, inserted, err := e.repository.createIfAbsent(ctx, record)
	if err != nil {
		return Record{}, err
	}
	if !inserted {
		if created.State == StateFailed || created.State == StateBlocked {
			return created, fmt.Errorf("%w: use retry for a %s replication", ErrConflict, created.State)
		}
		return created, nil
	}
	e.log.Info().Str("hash", created.Hash).Str("state", string(StatePending)).Int64("total_size", created.Manifest.TotalSize).Msg("torrent queued for replication")
	e.signal()
	return created, nil
}

func (e *Engine) Retry(ctx context.Context, hash string) (Record, error) {
	normalized, err := normalizeHash(hash)
	if err != nil {
		return Record{}, err
	}
	current, err := e.repository.get(ctx, normalized)
	if err != nil {
		return Record{}, err
	}
	if current.State != StateFailed && current.State != StateBlocked {
		return current, fmt.Errorf("%w: only failed or blocked replications can be retried", ErrConflict)
	}
	torrent, manifest, err := e.fetchManifest(ctx, normalized)
	if err != nil {
		var blocked *BlockedError
		if errors.As(err, &blocked) {
			if persistErr := e.recordBlocked(normalized, blocked.Error()); persistErr != nil {
				return current, errors.Join(err, persistErr)
			}
		}
		return current, err
	}
	updated := current
	updated.State = StatePending
	updated.Manifest = manifest
	updated.ManifestFingerprint = manifest.Fingerprint()
	updated.SourceIdentity = e.sourceIdentity()
	updated.DestinationIdentity = e.destinationIdentity()
	updated.UpdatedAt = time.Now().UTC()
	updated.NextAttemptAt = nil
	updated.VerifiedAt = nil
	updated.LastError = ""
	if torrent.Hash != "" {
		updated.Hash = strings.ToLower(torrent.Hash)
	}
	changed, err := e.repository.replace(ctx, updated, current.State)
	if err != nil {
		return Record{}, err
	}
	if !changed {
		fresh, getErr := e.repository.get(ctx, normalized)
		if getErr != nil {
			return Record{}, getErr
		}
		return fresh, fmt.Errorf("%w: replication changed while retry was being requested", ErrConflict)
	}
	updated, err = e.repository.get(ctx, normalized)
	if err != nil {
		return Record{}, err
	}
	e.log.Info().Str("hash", normalized).Str("state", string(StatePending)).Msg("replication retry queued")
	e.signal()
	return updated, nil
}

// Reconcile applies a successful discovery snapshot and queues newly completed
// torrents. It inspects file manifests only for new candidates and active work.
func (e *Engine) Reconcile(ctx context.Context, torrents []qbittorrent.Torrent) error {
	snapshot := make(map[string]qbittorrent.Torrent, len(torrents))
	for _, torrent := range torrents {
		if hash, err := normalizeHash(torrent.Hash); err == nil {
			snapshot[hash] = torrent
		}
	}

	e.mu.Lock()
	e.snapshot = snapshot
	e.connected = true
	activeHash := e.activeHash
	e.mu.Unlock()
	e.signal()

	if activeHash != "" {
		if err := e.reconcileActive(ctx, activeHash, snapshot); err != nil {
			return err
		}
		if !e.isConnected() {
			return nil
		}
	}

	for hash, torrent := range snapshot {
		if !potentiallyComplete(torrent) {
			continue
		}
		current, err := e.repository.get(ctx, hash)
		if err == nil {
			if current.State == StateReplicated && e.identityMismatch(current) {
				if _, blockErr := e.blockReconfigured(current); blockErr != nil {
					return blockErr
				}
			}
			continue
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}

		_, manifest, err := e.fetchManifest(ctx, hash)
		if err != nil {
			var blocked *BlockedError
			if errors.As(err, &blocked) {
				if persistErr := e.recordBlocked(hash, blocked.Error()); persistErr != nil {
					return persistErr
				}
			} else if !errors.Is(err, ErrIneligible) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				e.log.Warn().Err(err).Str("hash", hash).Msg("could not inspect replication candidate")
			}
			if errors.Is(err, ErrUnavailable) {
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		record := e.newRecord(hash, StatePending, manifest)
		created, inserted, err := e.repository.createIfAbsent(ctx, record)
		if err != nil {
			return err
		}
		if inserted {
			e.log.Info().Str("hash", hash).Str("state", string(StatePending)).Int64("total_size", manifest.TotalSize).Msg("torrent queued for replication")
			e.signal()
		} else if created.State == StateBlocked {
			// A concurrent manual request may have recorded a blocked condition.
			continue
		}
	}
	return nil
}

// Unavailable prevents the worker from taking new jobs until discovery succeeds.
func (e *Engine) Unavailable() {
	e.mu.Lock()
	e.connected = false
	e.mu.Unlock()
	e.signal()
}

func (e *Engine) Run(ctx context.Context) error {
	persistCtx, cancel := persistenceContext()
	err := e.repository.recoverInterrupted(persistCtx, time.Now().UTC())
	cancel()
	if err != nil {
		return err
	}
	e.signal()

	for {
		if ctx.Err() != nil {
			return nil
		}
		e.mu.Lock()
		connected := e.connected
		e.mu.Unlock()
		if connected {
			dbCtx, cancel := persistenceContext()
			record, found, err := e.repository.nextReady(dbCtx, time.Now().UTC())
			cancel()
			if err != nil {
				return err
			}
			if found {
				dbCtx, cancel = persistenceContext()
				started, ok, err := e.repository.start(dbCtx, record.Hash, time.Now().UTC())
				cancel()
				if err != nil {
					return err
				}
				if ok {
					if err := e.runRecord(ctx, started); err != nil {
						return err
					}
					continue
				}
			}
		}

		wait := time.Minute
		if connected {
			waitCtx, waitCancel := persistenceContext()
			due, err := e.repository.nextDue(waitCtx, time.Now().UTC())
			waitCancel()
			if err != nil {
				return err
			}
			if due != nil {
				wait = time.Until(*due)
				if wait < 0 {
					wait = 0
				}
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-e.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (e *Engine) runRecord(ctx context.Context, record Record) error {
	workCtx, cancel := context.WithCancel(ctx)
	e.mu.Lock()
	e.activeHash = record.Hash
	e.activeCancel = cancel
	connected := e.connected
	e.mu.Unlock()
	defer func() {
		cancel()
		e.mu.Lock()
		if e.activeHash == record.Hash {
			e.activeHash = ""
			e.activeCancel = nil
		}
		e.mu.Unlock()
	}()

	if !connected {
		return e.failRecord(record, ErrUnavailable)
	}
	if record.SourceIdentity != e.sourceIdentity() || record.DestinationIdentity != e.destinationIdentity() {
		return e.blockRecord(record, "replication source or destination configuration changed; retry to adopt the current paths")
	}
	e.log.Info().Str("hash", record.Hash).Str("state", string(StateTransferring)).Int("attempt", record.Attempts).Msg("replication transfer started")
	torrent, manifest, err := e.fetchManifest(workCtx, record.Hash)
	if err != nil {
		if isBlocked(err) || errors.Is(err, ErrIneligible) || errors.Is(err, qbittorrent.ErrNotFound) {
			return e.blockRecord(record, err.Error())
		}
		if workCtx.Err() != nil {
			return e.finishCanceled(record, ctx)
		}
		return e.failRecord(record, err)
	}
	if !record.Manifest.Equal(manifest) || record.ManifestFingerprint != manifest.Fingerprint() {
		return e.blockRecord(record, "torrent file manifest changed since this replication was queued")
	}
	if torrent.AutoTMM {
		return e.blockRecord(record, "automatic torrent management is enabled")
	}
	if !e.isConnected() {
		return e.failRecord(record, ErrUnavailable)
	}

	if err := e.runner.Transfer(workCtx, record.Manifest); err != nil {
		if workCtx.Err() != nil {
			return e.finishCanceled(record, ctx)
		}
		if isBlocked(err) || errors.Is(err, ErrIneligible) {
			return e.blockRecord(record, err.Error())
		}
		return e.failRecord(record, err)
	}
	if workCtx.Err() != nil {
		return e.finishCanceled(record, ctx)
	}
	changed, err := e.setRecordState(record, StateTransferring, StateVerifying, "", nil, nil)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	e.log.Info().Str("hash", record.Hash).Str("state", string(StateVerifying)).Msg("replication transfer completed")
	e.log.Info().Str("hash", record.Hash).Str("state", string(StateVerifying)).Msg("replication verification started")
	if err := e.runner.Verify(workCtx, record.Manifest); err != nil {
		if workCtx.Err() != nil {
			return e.finishCanceled(record, ctx)
		}
		if isBlocked(err) || errors.Is(err, ErrIneligible) {
			return e.blockRecord(record, err.Error())
		}
		return e.failRecord(record, err)
	}
	if workCtx.Err() != nil {
		return e.finishCanceled(record, ctx)
	}
	_, finalManifest, err := e.fetchManifest(workCtx, record.Hash)
	if err != nil {
		if isBlocked(err) || errors.Is(err, ErrIneligible) || errors.Is(err, qbittorrent.ErrNotFound) {
			return e.blockRecord(record, "torrent is no longer eligible after transfer: "+err.Error())
		}
		if workCtx.Err() != nil {
			return e.finishCanceled(record, ctx)
		}
		return e.failRecord(record, fmt.Errorf("verify torrent manifest after transfer: %w", err))
	}
	if !record.Manifest.Equal(finalManifest) || record.ManifestFingerprint != finalManifest.Fingerprint() {
		return e.blockRecord(record, "torrent file manifest changed during replication")
	}
	if err := e.markReplicated(record); err != nil {
		return err
	}
	return nil
}

func (e *Engine) reconcileActive(ctx context.Context, hash string, snapshot map[string]qbittorrent.Torrent) error {
	torrent, exists := snapshot[hash]
	if !exists {
		return e.cancelAndBlock(hash, "torrent was removed from qBittorrent")
	}
	if torrent.AutoTMM {
		return e.cancelAndBlock(hash, "automatic torrent management is enabled")
	}
	if !potentiallyComplete(torrent) {
		return e.cancelAndBlock(hash, "torrent is no longer in a completed state")
	}
	record, err := e.repository.get(ctx, hash)
	if err != nil {
		return err
	}
	_, manifest, err := e.fetchManifest(ctx, hash)
	if err != nil {
		if isBlocked(err) || errors.Is(err, ErrIneligible) {
			return e.cancelAndBlock(hash, err.Error())
		}
		if ctx.Err() != nil {
			return nil
		}
		e.log.Warn().Err(err).Str("hash", hash).Msg("could not revalidate active torrent manifest")
		return nil
	}
	if !record.Manifest.Equal(manifest) || record.ManifestFingerprint != manifest.Fingerprint() {
		return e.cancelAndBlock(hash, "torrent file manifest changed during replication")
	}
	return nil
}

func (e *Engine) cancelAndBlock(hash, reason string) error {
	if err := e.blockExisting(hash, reason); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	e.mu.Lock()
	if e.activeHash == hash && e.activeCancel != nil {
		e.activeCancel()
	}
	e.mu.Unlock()
	return nil
}

func (e *Engine) fetchManifest(ctx context.Context, hash string) (qbittorrent.Torrent, Manifest, error) {
	torrent, err := e.client.Torrent(ctx, hash)
	if err != nil {
		if errors.Is(err, qbittorrent.ErrNotFound) {
			return qbittorrent.Torrent{}, Manifest{}, fmt.Errorf("%w: torrent not found in qBittorrent: %w", ErrNotFound, qbittorrent.ErrNotFound)
		}
		if !errors.Is(err, context.Canceled) {
			e.Unavailable()
		}
		return qbittorrent.Torrent{}, Manifest{}, fmt.Errorf("%w: qBittorrent torrent lookup: %w", ErrUnavailable, err)
	}
	files, err := e.client.Files(ctx, hash)
	if err != nil {
		if errors.Is(err, qbittorrent.ErrNotFound) {
			return torrent, Manifest{}, fmt.Errorf("%w: torrent files not found in qBittorrent: %w", ErrNotFound, qbittorrent.ErrNotFound)
		}
		if !errors.Is(err, context.Canceled) {
			e.Unavailable()
		}
		return torrent, Manifest{}, fmt.Errorf("%w: qBittorrent file list: %w", ErrUnavailable, err)
	}
	manifest, err := BuildManifest(torrent, files, e.source)
	if err != nil {
		if errors.Is(err, ErrIneligible) || isBlocked(err) {
			return torrent, Manifest{}, err
		}
		return torrent, Manifest{}, fmt.Errorf("%w: build torrent manifest: %w", ErrUnavailable, err)
	}
	return torrent, manifest, nil
}

func (e *Engine) recordBlocked(hash, reason string) error {
	dbCtx, cancel := persistenceContext()
	defer cancel()
	current, err := e.repository.get(dbCtx, hash)
	if errors.Is(err, ErrNotFound) {
		record := e.newRecord(hash, StateBlocked, Manifest{})
		record.LastError = reason
		created, inserted, err := e.repository.createIfAbsent(dbCtx, record)
		if err != nil || created.State == StateReplicated {
			return err
		}
		if inserted {
			e.log.Warn().Str("hash", hash).Str("state", string(StateBlocked)).Str("reason", reason).Msg("replication blocked")
			return nil
		}
		oldState := created.State
		created.State = StateBlocked
		created.LastError = reason
		created.NextAttemptAt = nil
		created.UpdatedAt = time.Now().UTC()
		_, err = e.repository.replace(dbCtx, created, oldState)
		if err == nil {
			e.log.Warn().Str("hash", hash).Str("state", string(StateBlocked)).Str("reason", reason).Msg("replication blocked")
		}
		return err
	}
	if err != nil {
		return err
	}
	if current.State == StateReplicated {
		return nil
	}
	oldState := current.State
	current.State = StateBlocked
	current.LastError = reason
	current.NextAttemptAt = nil
	current.UpdatedAt = time.Now().UTC()
	changed, err := e.repository.replace(dbCtx, current, oldState)
	if err != nil {
		return err
	}
	if changed {
		e.log.Warn().Str("hash", hash).Str("state", string(StateBlocked)).Str("reason", reason).Msg("replication blocked")
		e.signal()
	}
	return nil
}

func (e *Engine) identityMismatch(record Record) bool {
	return record.SourceIdentity != e.sourceIdentity() || record.DestinationIdentity != e.destinationIdentity()
}

func (e *Engine) blockReconfigured(record Record) (Record, error) {
	updated := record
	updated.State = StateBlocked
	updated.LastError = "replicated copy belongs to a different source or destination configuration; retry to replicate to the current paths"
	updated.NextAttemptAt = nil
	updated.UpdatedAt = time.Now().UTC()
	dbCtx, cancel := persistenceContext()
	defer cancel()
	changed, err := e.repository.replace(dbCtx, updated, StateReplicated)
	if err != nil {
		return Record{}, err
	}
	if !changed {
		return e.repository.get(dbCtx, record.Hash)
	}
	e.log.Warn().Str("hash", record.Hash).Str("state", string(StateBlocked)).Msg("replication blocked after configuration change")
	e.signal()
	return e.repository.get(dbCtx, record.Hash)
}

func (e *Engine) blockExisting(hash, reason string) error {
	dbCtx, cancel := persistenceContext()
	defer cancel()
	current, err := e.repository.get(dbCtx, hash)
	if err != nil {
		return err
	}
	if current.State == StateReplicated || current.State == StateBlocked {
		return nil
	}
	oldState := current.State
	current.State = StateBlocked
	current.LastError = reason
	current.NextAttemptAt = nil
	current.UpdatedAt = time.Now().UTC()
	changed, err := e.repository.replace(dbCtx, current, oldState)
	if err != nil {
		return err
	}
	if changed {
		e.log.Warn().Str("hash", hash).Str("state", string(StateBlocked)).Str("reason", reason).Msg("replication blocked")
		e.signal()
	}
	return nil
}

func (e *Engine) blockRecord(record Record, reason string) error {
	if err := e.blockExisting(record.Hash, reason); err != nil {
		return err
	}
	return nil
}

func (e *Engine) setRecordState(record Record, oldState, newState State, lastError string, nextAttempt, verifiedAt *time.Time) (bool, error) {
	updated := record
	updated.State = newState
	updated.LastError = lastError
	updated.NextAttemptAt = nextAttempt
	updated.VerifiedAt = verifiedAt
	updated.UpdatedAt = time.Now().UTC()
	dbCtx, cancel := persistenceContext()
	changed, err := e.repository.replace(dbCtx, updated, oldState)
	cancel()
	if err != nil {
		return false, err
	}
	e.signal()
	return changed, nil
}

func (e *Engine) markReplicated(record Record) error {
	updated := record
	updated.State = StateReplicated
	updated.UpdatedAt = time.Now().UTC()
	updated.VerifiedAt = ptrTime(updated.UpdatedAt)
	updated.NextAttemptAt = nil
	updated.LastError = ""
	dbCtx, cancel := persistenceContext()
	changed, err := e.repository.replace(dbCtx, updated, StateVerifying)
	cancel()
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	e.log.Info().Str("hash", record.Hash).Str("state", string(StateReplicated)).Msg("replication verified")
	e.signal()
	return nil
}

func (e *Engine) failRecord(record Record, cause error) error {
	currentCtx, cancel := persistenceContext()
	defer cancel()
	current, err := e.repository.get(currentCtx, record.Hash)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if current.State != StateTransferring && current.State != StateVerifying {
		return nil
	}
	updated := current
	updated.State = StateFailed
	updated.LastError = cause.Error()
	updated.NextAttemptAt = ptrTime(time.Now().UTC().Add(retryDelay(updated.Attempts)))
	updated.UpdatedAt = time.Now().UTC()
	changed, err := e.repository.replace(currentCtx, updated, current.State)
	if err != nil {
		return err
	}
	if changed {
		e.log.Warn().Err(cause).Str("hash", record.Hash).Str("state", string(StateFailed)).Dur("retry_in", time.Until(*updated.NextAttemptAt)).Msg("replication failed; retry scheduled")
		e.signal()
	}
	return nil
}

func (e *Engine) finishCanceled(record Record, runCtx context.Context) error {
	if runCtx.Err() == nil {
		return nil
	}
	return e.failRecord(record, errors.New("transfer interrupted by daemon shutdown"))
}

func (e *Engine) newRecord(hash string, state State, manifest Manifest) Record {
	now := time.Now().UTC()
	return Record{
		Hash:                strings.ToLower(hash),
		State:               state,
		Manifest:            manifest,
		ManifestFingerprint: manifest.Fingerprint(),
		SourceIdentity:      e.sourceIdentity(),
		DestinationIdentity: e.destinationIdentity(),
		CreatedAt:           now,
		UpdatedAt:           now,
	}
}

func (e *Engine) sourceIdentity() string {
	return fmt.Sprintf("%q -> %q", e.source.QBittorrentBasePath, e.source.LocalBasePath)
}

func (e *Engine) destinationIdentity() string {
	return fmt.Sprintf("%s:%q", e.destination.SSHTarget, e.destination.BasePath)
}

func (e *Engine) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *Engine) isConnected() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.connected
}

func normalizeHash(hash string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(hash))
	if normalized == "" {
		return "", fmt.Errorf("%w: empty torrent hash", ErrNotFound)
	}
	return normalized, nil
}

func potentiallyComplete(torrent qbittorrent.Torrent) bool {
	state := strings.ToLower(strings.TrimSpace(torrent.State))
	if state == "missingfiles" {
		return true
	}
	if state == "" || strings.Contains(state, "checking") || strings.Contains(state, "moving") || strings.Contains(state, "missing") || strings.Contains(state, "error") || strings.Contains(state, "allocating") {
		return false
	}
	if state == "uploading" || strings.HasSuffix(state, "up") {
		return true
	}
	if strings.HasSuffix(state, "dl") {
		return false
	}
	return torrent.Progress >= 1
}

func isBlocked(err error) bool {
	var blocked *BlockedError
	return errors.As(err, &blocked)
}

func retryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	delay := baseRetryDelay
	for i := 1; i < attempts && delay < maxRetryDelay; i++ {
		delay *= 2
	}
	if delay > maxRetryDelay {
		return maxRetryDelay
	}
	return delay
}

func persistenceContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), databaseTimeout)
}

func ptrTime(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}
