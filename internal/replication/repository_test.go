package replication

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/anish-chanda/seedcache/internal/config"
	"github.com/anish-chanda/seedcache/internal/qbittorrent"
	"github.com/anish-chanda/seedcache/internal/store"
	"github.com/rs/zerolog"
)

func TestRepositoryPersistsAndRecoversReplication(t *testing.T) {
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "seedcache.db"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := &repository{db: db}

	now := time.Date(2026, time.November, 15, 12, 30, 0, 123456789, time.FixedZone("offset", -6*60*60))
	manifest := Manifest{Files: []File{{Path: "Movies/Example.mkv", Size: 123, Index: 0}}, TotalSize: 123}
	initial := Record{
		Hash:                "abc123",
		State:               StatePending,
		Manifest:            manifest,
		ManifestFingerprint: manifest.Fingerprint(),
		SourceIdentity:      "source-a",
		DestinationIdentity: "destination-a",
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	created, inserted, err := repo.createIfAbsent(context.Background(), initial)
	if err != nil || !inserted {
		t.Fatalf("createIfAbsent() = (%+v, %t, %v), want inserted record", created, inserted, err)
	}
	if !created.CreatedAt.Equal(now) || !created.Manifest.Equal(manifest) {
		t.Fatalf("persisted record = %+v, want original timestamp and manifest", created)
	}

	ready, found, err := repo.nextReady(context.Background(), now)
	if err != nil || !found || ready.Hash != initial.Hash {
		t.Fatalf("nextReady() = (%+v, %t, %v), want pending record", ready, found, err)
	}

	started, ok, err := repo.start(context.Background(), initial.Hash, now)
	if err != nil || !ok || started.State != StateTransferring || started.Attempts != 1 || started.LastAttemptAt == nil {
		t.Fatalf("start() = (%+v, %t, %v), want first transfer attempt", started, ok, err)
	}
	if _, found, err := repo.nextReady(context.Background(), now); err != nil || found {
		t.Fatalf("nextReady() after start = found %t, error %v; want no ready job", found, err)
	}

	if err := repo.recoverInterrupted(context.Background(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	recovered, err := repo.get(context.Background(), initial.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.State != StateFailed || recovered.NextAttemptAt == nil || !recovered.NextAttemptAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("recovered state = %+v, want retryable failed record", recovered)
	}
	if recovered.LastError != "transfer interrupted by daemon restart" {
		t.Errorf("recovery error = %q", recovered.LastError)
	}

	summary, err := repo.summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.Failed != 1 || summary.Pending != 0 || summary.Active != 0 {
		t.Errorf("summary after recovery = %+v", summary)
	}
}

func TestRepositoryEmptyQueueAndDuplicateInsert(t *testing.T) {
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "seedcache.db"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := &repository{db: db}

	if _, found, err := repo.nextReady(context.Background(), time.Now()); err != nil || found {
		t.Fatalf("empty nextReady() = found %t, error %v", found, err)
	}
	manifest := Manifest{Files: []File{{Path: "file.bin", Size: 7, Index: 0}}, TotalSize: 7}
	record := Record{Hash: "def456", State: StatePending, Manifest: manifest, ManifestFingerprint: manifest.Fingerprint(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if _, inserted, err := repo.createIfAbsent(context.Background(), record); err != nil || !inserted {
		t.Fatalf("first insert = inserted %t, error %v", inserted, err)
	}
	duplicate, inserted, err := repo.createIfAbsent(context.Background(), record)
	if err != nil || inserted || duplicate.Hash != record.Hash {
		t.Fatalf("duplicate insert = (%+v, %t, %v), want existing record", duplicate, inserted, err)
	}
	if _, err := repo.get(context.Background(), "unknown"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get unknown error = %v, want ErrNotFound", err)
	}
}

func TestRepositoryPersistsAndRecoversCacheOperations(t *testing.T) {
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "cache.db"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := &repository{db: db}
	now := time.Now().UTC().Truncate(time.Nanosecond)
	manifest := Manifest{Files: []File{{Path: "media/file.bin", Size: 19, Index: 0}}, TotalSize: 19}
	initial := Record{
		Hash:                "abc123",
		State:               StateReplicated,
		Manifest:            manifest,
		ManifestFingerprint: manifest.Fingerprint(),
		SourceIdentity:      "source",
		DestinationIdentity: "destination",
		CreatedAt:           now,
		UpdatedAt:           now,
		VerifiedAt:          &now,
	}
	created, inserted, err := repo.createIfAbsent(context.Background(), initial)
	if err != nil || !inserted {
		t.Fatalf("createIfAbsent() = inserted %t, err %v", inserted, err)
	}
	if created.CacheState != CachePresent || !created.CacheUpdatedAt.Equal(now) {
		t.Fatalf("default cache state = %+v, want present at creation time", created)
	}
	queued := created
	queued.CacheOperation = CacheOperationEvict
	queued.CacheUpdatedAt = now.Add(time.Second)
	changed, err := repo.replaceCache(context.Background(), created, queued)
	if err != nil || !changed {
		t.Fatalf("replaceCache() = changed %t, err %v", changed, err)
	}
	operation, found, err := repo.nextCacheOperation(context.Background())
	if err != nil || !found || operation.CacheOperation != CacheOperationEvict {
		t.Fatalf("nextCacheOperation() = (%+v, %t, %v)", operation, found, err)
	}
	withError := operation
	withError.CacheError = "interrupted"
	withError.CacheUpdatedAt = now.Add(2 * time.Second)
	if changed, err := repo.replaceCache(context.Background(), operation, withError); err != nil || !changed {
		t.Fatalf("persist cache error = changed %t, err %v", changed, err)
	}
	if _, found, err := repo.nextCacheOperation(context.Background()); err != nil || found {
		t.Fatalf("failed operation was runnable before recovery: found=%t err=%v", found, err)
	}
	if err := repo.recoverCacheOperations(context.Background(), now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	recovered, found, err := repo.nextCacheOperation(context.Background())
	if err != nil || !found || recovered.CacheError != "" || recovered.CacheOperation != CacheOperationEvict {
		t.Fatalf("recovered cache operation = (%+v, %t, %v)", recovered, found, err)
	}
}

func TestRetryDelayIsExponentialAndBounded(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: 30 * time.Second},
		{attempt: 1, want: 30 * time.Second},
		{attempt: 2, want: time.Minute},
		{attempt: 5, want: 8 * time.Minute},
		{attempt: 20, want: 30 * time.Minute},
	}
	for _, tt := range tests {
		if got := retryDelay(tt.attempt); got != tt.want {
			t.Errorf("retryDelay(%d) = %s, want %s", tt.attempt, got, tt.want)
		}
	}
}

func TestPotentiallyCompleteIncludesMissingFilesForBlocking(t *testing.T) {
	if !potentiallyComplete(qbTorrent("missingFiles", 0)) {
		t.Fatal("missingFiles state should be inspected so the transfer can be blocked with a useful reason")
	}
	for _, state := range []string{"checkingUP", "moving", "error", "stalledDL"} {
		if potentiallyComplete(qbTorrent(state, 1)) {
			t.Errorf("potentiallyComplete(%q) = true, want false", state)
		}
	}
	for _, state := range []string{"uploading", "stoppedUP", "stalledUP"} {
		if !potentiallyComplete(qbTorrent(state, 0.7)) {
			t.Errorf("potentiallyComplete(%q) = false, want true", state)
		}
	}
}

func TestReconfiguredVerifiedRecordKeepsItsVerifiedManifest(t *testing.T) {
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "seedcache.db"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := &repository{db: db}
	verifiedAt := time.Now().UTC().Add(-time.Hour)
	manifest := Manifest{Files: []File{{Path: "folder/file.bin", Size: 42, Index: 3}}, TotalSize: 42}
	record := Record{
		Hash:                "ghi789",
		State:               StateReplicated,
		Manifest:            manifest,
		ManifestFingerprint: manifest.Fingerprint(),
		SourceIdentity:      "old-source",
		DestinationIdentity: "old-destination",
		CreatedAt:           verifiedAt,
		UpdatedAt:           verifiedAt,
		VerifiedAt:          &verifiedAt,
	}
	if _, inserted, err := repo.createIfAbsent(context.Background(), record); err != nil || !inserted {
		t.Fatalf("create verified row: inserted=%t err=%v", inserted, err)
	}
	engine := &Engine{
		repository:  repo,
		source:      config.SourceConfig{QBittorrentBasePath: "/downloads-new", LocalBasePath: "/data-new"},
		destination: config.DestinationConfig{SSHTarget: "new-host", BasePath: "/storage-new"},
		wake:        make(chan struct{}, 1),
	}
	blocked, err := engine.blockReconfigured(record)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.State != StateBlocked || blocked.VerifiedAt == nil || !blocked.VerifiedAt.Equal(verifiedAt) {
		t.Fatalf("reconfigured record = %+v, want blocked with original verification timestamp", blocked)
	}
	if !blocked.Manifest.Equal(manifest) || blocked.SourceIdentity != "old-source" || blocked.DestinationIdentity != "old-destination" {
		t.Fatalf("verified copy metadata changed during blocking: %+v", blocked)
	}
}

func qbTorrent(state string, progress float64) qbittorrent.Torrent {
	return qbittorrent.Torrent{State: state, Progress: progress}
}
