package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/anish-chanda/seedcache/internal/api"
	"github.com/anish-chanda/seedcache/internal/config"
	"github.com/anish-chanda/seedcache/internal/logger"
	"github.com/anish-chanda/seedcache/internal/qbittorrent"
	"github.com/anish-chanda/seedcache/internal/replication"
	"github.com/anish-chanda/seedcache/internal/store"
)

func Run(ctx context.Context, path, version string) (result error) {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	log, err := logger.New(cfg.Logging.Level)
	if err != nil {
		return err
	}
	startup, cancelStartup := context.WithTimeout(ctx, 15*time.Second)
	db, err := store.Open(startup, cfg.Database.Path, log.With().Str("component", "store").Logger())
	cancelStartup()
	if err != nil {
		return err
	}
	defer func() {
		if err := db.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close database: %w", err))
		}
	}()
	client, err := qbittorrent.New(cfg.QBittorrent.URL, cfg.QBittorrent.Username, cfg.QBittorrent.Password)
	if err != nil {
		return err
	}
	var engine *replication.Engine
	if cfg.ReplicationEnabled() {
		engine, err = replication.NewEngine(db, client, cfg.Source, cfg.Destination, log.With().Str("component", "replication").Logger())
		if err != nil {
			return fmt.Errorf("initialize replication: %w", err)
		}
	}
	listener, err := net.Listen("tcp", cfg.API.Listen)
	if err != nil {
		return fmt.Errorf("listen API: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	discovery := newDiscovery(client, log.With().Str("component", "discovery").Logger())
	discovery.engine = engine
	var transfers []api.Transfers
	if engine != nil {
		transfers = append(transfers, engine)
	}
	server := &http.Server{
		Handler:           api.New(discovery, version, log.With().Str("component", "api").Logger(), transfers...),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      40 * time.Second,
		IdleTimeout:       60 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		discovery.run(ctx, cfg.Discovery.Interval)
	}()
	served := make(chan error, 1)
	replicationDone := make(chan struct{})
	replicationErrors := make(chan error, 1)
	go func() {
		defer close(replicationDone)
		if engine != nil {
			if err := engine.Run(ctx); err != nil {
				replicationErrors <- err
			}
		}
	}()
	go func() { served <- server.Serve(listener) }()
	log.Info().Str("address", listener.Addr().String()).Str("version", version).Msg("SeedCache started")
	select {
	case <-ctx.Done():
	case err := <-replicationErrors:
		result = fmt.Errorf("replication worker: %w", err)
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			result = fmt.Errorf("serve API: %w", err)
		}
	}
	cancel()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdown); err != nil {
		result = errors.Join(result, fmt.Errorf("shutdown API: %w", err))
		if closeErr := server.Close(); closeErr != nil {
			result = errors.Join(result, closeErr)
		}
	}
	<-done
	<-replicationDone
	log.Info().Msg("SeedCache stopped")
	return result
}
