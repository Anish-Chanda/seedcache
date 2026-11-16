package replication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/anish-chanda/seedcache/internal/qbittorrent"
)

type adoptionReservation struct {
	hashes    []string
	conflicts map[string]error
}

func (e *Engine) beginBulkAdoption(ctx context.Context) ([]qbittorrent.Torrent, *adoptionReservation, error) {
	if err := e.acquireCatalog(ctx); err != nil {
		return nil, nil, err
	}
	defer e.releaseCatalog()
	e.mu.Lock()
	active := map[string]bool{e.activeHash: true, e.activeCacheHash: true}
	e.mu.Unlock()
	torrents, err := e.client.Torrents(ctx)
	if err != nil {
		return nil, nil, err
	}
	hashes := make([]string, 0, len(torrents))
	for _, torrent := range torrents {
		hashes = append(hashes, strings.ToLower(strings.TrimSpace(torrent.Hash)))
	}
	reservation, err := e.reserveAdoptions(ctx, hashes, active)
	if err != nil {
		return nil, nil, err
	}
	return torrents, reservation, nil
}

// The catalog gate is held while checking operations and installing reservations.
func (e *Engine) reserveAdoptions(ctx context.Context, hashes []string, initiallyActive map[string]bool) (*adoptionReservation, error) {
	reservation := &adoptionReservation{conflicts: make(map[string]error)}
	seen := make(map[string]bool)
	for _, hash := range hashes {
		hash = strings.ToLower(strings.TrimSpace(hash))
		if seen[hash] {
			continue
		}
		seen[hash] = true
		var state State
		var cache CacheState
		var operation CacheOperation
		err := e.repository.db.QueryRowContext(ctx, "SELECT state,cache_state,cache_operation FROM replications WHERE hash = ?", hash).Scan(&state, &cache, &operation)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		e.mu.Lock()
		active := initiallyActive[hash] || e.activeHash == hash || e.activeCacheHash == hash
		e.mu.Unlock()
		if active || state == StateTransferring || state == StateVerifying || operation != "" || cache == CacheEvicting || cache == CacheRestoring {
			reservation.conflicts[hash] = fmt.Errorf("%w: torrent operation was already active, queued, or requires recovery", ErrConflict)
		} else if e.adoptionReservations[hash] > 0 {
			reservation.conflicts[hash] = fmt.Errorf("%w: torrent is reserved by another adoption", ErrConflict)
		}
		reservation.hashes = append(reservation.hashes, hash)
	}
	for _, hash := range reservation.hashes {
		e.adoptionReservations[hash]++
	}
	e.signal()
	return reservation, nil
}

func (e *Engine) releaseAdoptions(reservation *adoptionReservation) {
	_ = e.acquireCatalog(context.Background())
	for _, hash := range reservation.hashes {
		if e.adoptionReservations[hash] <= 1 {
			delete(e.adoptionReservations, hash)
		} else {
			e.adoptionReservations[hash]--
		}
	}
	e.releaseCatalog()
	e.signal()
}

func (e *Engine) reservedAdoptionHashes() []string {
	hashes := make([]string, 0, len(e.adoptionReservations))
	for hash := range e.adoptionReservations {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	return hashes
}
