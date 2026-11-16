package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/anish-chanda/seedcache/internal/api"
	"github.com/anish-chanda/seedcache/internal/qbittorrent"
	"github.com/anish-chanda/seedcache/internal/replication"
	"github.com/rs/zerolog"
)

type discovery struct {
	client   *qbittorrent.Client
	log      zerolog.Logger
	mu       sync.RWMutex
	status   api.Status
	torrents []api.Torrent
	engine   *replication.Engine
}

func newDiscovery(client *qbittorrent.Client, log zerolog.Logger) *discovery {
	return &discovery{
		client: client,
		log:    log,
		status: api.Status{
			StartedAt:   time.Now().UTC(),
			QBittorrent: api.Connection{Error: "discovery has not completed"},
		},
		torrents: []api.Torrent{},
	}
}

func (d *discovery) Status() api.Status {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.status
}

func (d *discovery) Torrents() ([]api.Torrent, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if !d.status.QBittorrent.Connected {
		return nil, api.ErrUnavailable
	}
	return append([]api.Torrent{}, d.torrents...), nil
}

func (d *discovery) run(ctx context.Context, interval time.Duration) {
	d.poll(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.poll(ctx)
		}
	}
}

func (d *discovery) poll(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	version, apiVersion, err := d.client.Versions(ctx)
	var prefs qbittorrent.Preferences
	var torrents []qbittorrent.Torrent
	if err == nil {
		prefs, err = d.client.Preferences(ctx)
	}
	if err == nil {
		torrents, err = d.client.Torrents(ctx)
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return
	}
	if err != nil {
		d.disconnected(err)
		return
	}
	snapshot := make([]api.Torrent, 0, len(torrents))
	for _, torrent := range torrents {
		snapshot = append(snapshot, mapTorrent(torrent))
	}
	now := time.Now().UTC()
	d.mu.Lock()
	wasConnected := d.status.QBittorrent.Connected
	d.status.QBittorrent = api.Connection{Connected: true, Version: version, APIVersion: apiVersion, LastSuccess: &now, AutoTMMEnabled: prefs.AutoTMMEnabled}
	d.torrents = snapshot
	d.status.TorrentCount = len(snapshot)
	d.mu.Unlock()
	if !wasConnected {
		d.log.Info().Str("version", version).Msg("qBittorrent connected")
	}
	d.log.Debug().Int("torrents", len(snapshot)).Msg("Discovery refreshed")
	if d.engine != nil {
		if err := d.engine.Reconcile(ctx, torrents); err != nil && ctx.Err() == nil {
			d.log.Error().Err(err).Msg("Reconcile replications failed")
		}
	}
}

func (d *discovery) disconnected(err error) {
	if d.engine != nil {
		d.engine.Unavailable()
	}
	d.mu.Lock()
	previous := d.status.QBittorrent.Error
	connected := d.status.QBittorrent.Connected
	d.status.QBittorrent.Connected = false
	d.status.QBittorrent.Error = err.Error()
	d.mu.Unlock()
	if connected || previous != err.Error() {
		d.log.Warn().Err(err).Msg("qBittorrent discovery unavailable")
	}
}

func (d *discovery) Detail(ctx context.Context, hash string) (api.TorrentDetail, error) {
	torrent, err := d.client.Torrent(ctx, hash)
	if err != nil {
		return api.TorrentDetail{}, d.detailError(err)
	}
	files, err := d.client.Files(ctx, hash)
	if err != nil {
		return api.TorrentDetail{}, d.detailError(err)
	}
	detail := api.TorrentDetail{Torrent: mapTorrent(torrent), Files: make([]api.File, 0, len(files))}
	for _, file := range files {
		detail.Files = append(detail.Files, api.File{Index: file.Index, Path: file.Name, Size: file.Size, Priority: file.Priority, Progress: file.Progress})
	}
	return detail, nil
}

func (d *discovery) detailError(err error) error {
	if errors.Is(err, qbittorrent.ErrNotFound) {
		return api.ErrNotFound
	}
	if !errors.Is(err, context.Canceled) {
		d.disconnected(err)
	}
	return fmt.Errorf("%w: %v", api.ErrUnavailable, err)
}

func mapTorrent(t qbittorrent.Torrent) api.Torrent {
	result := api.Torrent{
		Hash:           t.Hash,
		Name:           t.Name,
		State:          t.State,
		Progress:       t.Progress,
		TotalSize:      t.TotalSize,
		Uploaded:       t.Uploaded,
		Ratio:          t.Ratio,
		SeedingSeconds: t.SeedingTime,
		SavePath:       t.SavePath,
		AutoTMM:        t.AutoTMM,
		CacheState:     "present",
	}
	if t.CompletionOn > 0 {
		completed := time.Unix(t.CompletionOn, 0).UTC()
		result.CompletedAt = &completed
	}
	return result
}
