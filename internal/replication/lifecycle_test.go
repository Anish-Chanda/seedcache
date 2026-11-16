package replication

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anish-chanda/seedcache/internal/config"
	"github.com/anish-chanda/seedcache/internal/qbittorrent"
	"github.com/anish-chanda/seedcache/internal/store"
	"github.com/rs/zerolog"
)

func TestRemoveManifestFilesDeletesOnlyListedPayload(t *testing.T) {
	root := t.TempDir()
	payloadDir := filepath.Join(root, "media")
	if err := os.Mkdir(payloadDir, 0o750); err != nil {
		t.Fatal(err)
	}
	selected := filepath.Join(payloadDir, "selected.bin")
	unrelated := filepath.Join(payloadDir, "unrelated.txt")
	if err := os.WriteFile(selected, []byte("selected"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unrelated, []byte("keep"), 0o640); err != nil {
		t.Fatal(err)
	}
	source := config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: root}
	manifest := Manifest{Files: []File{{Path: "media/selected.bin", Size: int64(len("selected")), Index: 0}}, TotalSize: int64(len("selected"))}

	if err := removeManifestFiles(source, manifest, false); err != nil {
		t.Fatalf("removeManifestFiles() error = %v", err)
	}
	if _, err := os.Stat(selected); !os.IsNotExist(err) {
		t.Fatalf("selected payload still exists or stat failed: %v", err)
	}
	if contents, err := os.ReadFile(unrelated); err != nil || string(contents) != "keep" {
		t.Fatalf("unrelated file changed: contents=%q err=%v", contents, err)
	}
	if info, err := os.Stat(payloadDir); err != nil || !info.IsDir() {
		t.Fatalf("containing directory was removed or changed: info=%v err=%v", info, err)
	}
}

func TestRemoveManifestFilesResumesPartialEviction(t *testing.T) {
	root := t.TempDir()
	paths := []string{"part-a.bin", "part-b.bin"}
	files := make([]File, 0, len(paths))
	for index, name := range paths {
		contents := []byte(name + " contents")
		if err := os.WriteFile(filepath.Join(root, name), contents, 0o640); err != nil {
			t.Fatal(err)
		}
		files = append(files, File{Path: name, Size: int64(len(contents)), Index: index})
	}
	manifest := Manifest{Files: files, TotalSize: files[0].Size + files[1].Size}
	source := config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: root}
	if err := os.Remove(filepath.Join(root, paths[0])); err != nil {
		t.Fatal(err)
	}
	if err := removeManifestFiles(source, manifest, false); err == nil {
		t.Fatal("initial eviction accepted a missing manifest file")
	}
	if err := removeManifestFiles(source, manifest, true); err != nil {
		t.Fatalf("recover interrupted eviction: %v", err)
	}
	for _, name := range paths {
		if _, err := os.Lstat(filepath.Join(root, name)); !os.IsNotExist(err) {
			t.Errorf("manifest file %q remains or stat failed: %v", name, err)
		}
	}
}

func TestRemoveManifestFilesRefusesSymlinkTraversal(t *testing.T) {
	root := t.TempDir()
	external := t.TempDir()
	victim := filepath.Join(external, "victim.bin")
	if err := os.WriteFile(victim, []byte("keep"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Files: []File{{Path: "linked/victim.bin", Size: 4, Index: 0}}, TotalSize: 4}
	source := config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: root}
	if err := removeManifestFiles(source, manifest, false); err == nil {
		t.Fatal("eviction followed a symlinked parent directory")
	}
	if contents, err := os.ReadFile(victim); err != nil || string(contents) != "keep" {
		t.Fatalf("external payload changed: contents=%q err=%v", contents, err)
	}
}

func TestManifestFromTorrentMetadataSupportsMissingEvictedPayload(t *testing.T) {
	root := t.TempDir()
	source := config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: root}
	torrent := qbittorrent.Torrent{
		Hash:        "hash-a",
		State:       "missingFiles",
		Size:        0,
		SavePath:    "/downloads",
		ContentPath: "/downloads/Example.bin",
	}
	files := []qbittorrent.File{{Index: 0, Name: "Example.bin", Size: 100, Priority: 1, Progress: 0}}
	manifest, err := manifestFromTorrentMetadata(torrent, files, source)
	if err != nil {
		t.Fatalf("manifestFromTorrentMetadata() error = %v", err)
	}
	want := Manifest{Files: []File{{Path: "Example.bin", Size: 100, Index: 0}}, TotalSize: 100}
	if !manifest.Equal(want) {
		t.Fatalf("metadata manifest = %+v, want %+v", manifest, want)
	}
}

func TestCheckNoSharedPayloadIncludesIncompleteUnselectedFiles(t *testing.T) {
	payload := strings.Repeat("x", 128)
	candidate := singleFileTorrent("hash-a", "shared.bin", payload)
	owner := singleFileTorrent("hash-b", "shared.bin", payload)
	owner.torrent.State = "downloading"
	owner.torrent.Progress = 0.25
	owner.torrent.AmountLeft = int64(len(payload))
	owner.files[0].Priority = 0
	owner.files[0].Progress = 0
	env := newSchedulerTestEnv(t, []testTorrent{candidate, owner})

	_, manifest, err := env.engine.fetchManifest(context.Background(), "hash-a")
	if err != nil {
		t.Fatalf("fetch candidate manifest: %v", err)
	}
	err = env.engine.checkNoSharedPayload(context.Background(), "hash-a", manifest, false)
	if !isBlocked(err) {
		t.Fatalf("checkNoSharedPayload() error = %v, want shared-file block", err)
	}
}

func TestCheckNoSharedPayloadDetectsHardLinks(t *testing.T) {
	payload := strings.Repeat("y", 128)
	candidate := singleFileTorrent("hash-a", "selected.bin", payload)
	owner := singleFileTorrent("hash-b", "other-name.bin", payload)
	env := newSchedulerTestEnv(t, []testTorrent{candidate, owner})
	selected := filepath.Join(env.localRoot, "selected.bin")
	other := filepath.Join(env.localRoot, "other-name.bin")
	if err := os.Remove(other); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(selected, other); err != nil {
		t.Fatal(err)
	}

	_, manifest, err := env.engine.fetchManifest(context.Background(), "hash-a")
	if err != nil {
		t.Fatalf("fetch candidate manifest: %v", err)
	}
	err = env.engine.checkNoSharedPayload(context.Background(), "hash-a", manifest, false)
	if !isBlocked(err) {
		t.Fatalf("checkNoSharedPayload() error = %v, want hard-link block", err)
	}
}

func TestCheckNoSharedPayloadBlocksUnmappedTorrentOwnership(t *testing.T) {
	payload := strings.Repeat("z", 128)
	candidate := singleFileTorrent("hash-a", "selected.bin", payload)
	owner := singleFileTorrent("hash-b", "outside.bin", payload)
	owner.torrent.SavePath = "/elsewhere"
	owner.torrent.DownloadPath = "/elsewhere"
	owner.torrent.ContentPath = "/elsewhere/outside.bin"
	env := newSchedulerTestEnv(t, []testTorrent{candidate, owner})
	_, manifest, err := env.engine.fetchManifest(context.Background(), "hash-a")
	if err != nil {
		t.Fatalf("fetch candidate manifest: %v", err)
	}
	if err := env.engine.checkNoSharedPayload(context.Background(), "hash-a", manifest, false); !isBlocked(err) {
		t.Fatalf("checkNoSharedPayload() error = %v, want ambiguous ownership block", err)
	}
}

const lifecycleTestHash = "0123456789abcdef0123456789abcdef01234567"

func TestEvictQueuesThenDeletesOnlyManifestPayloadAndRetainsTorrent(t *testing.T) {
	env := newLifecycleTestEnv(t, CachePresent, true)
	queued, err := env.engine.Evict(context.Background(), lifecycleTestHash)
	if err != nil {
		t.Fatalf("Evict() error = %v", err)
	}
	if queued.CacheState != CachePresent || queued.CacheOperation != CacheOperationEvict {
		t.Fatalf("Evict() = %+v, want queued operation with payload still present", queued)
	}
	selected := filepath.Join(env.sourceRoot, "media", "payload.bin")
	unselected := filepath.Join(env.sourceRoot, "media", "keep.txt")
	if _, err := os.Stat(selected); err != nil {
		t.Fatalf("asynchronous Evict() deleted files before worker execution: %v", err)
	}
	if err := env.engine.runCacheOperation(context.Background(), queued); err != nil {
		t.Fatalf("run eviction: %v", err)
	}
	record, err := env.engine.Get(context.Background(), lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if record.CacheState != CacheEvicted || record.CacheOperation != "" || record.EvictedAt == nil {
		t.Fatalf("eviction record = %+v, want evicted with timestamp", record)
	}
	if _, err := os.Lstat(selected); !os.IsNotExist(err) {
		t.Fatalf("selected payload remains or stat failed: %v", err)
	}
	if contents, err := os.ReadFile(unselected); err != nil || string(contents) != "keep" {
		t.Fatalf("unselected payload changed: contents=%q err=%v", contents, err)
	}
	if _, err := env.client.Torrent(context.Background(), lifecycleTestHash); err != nil {
		t.Fatalf("qBittorrent torrent entry was removed: %v", err)
	}
	if env.api.count("delete") != 0 {
		t.Fatalf("qBittorrent delete endpoint called %d times", env.api.count("delete"))
	}
}

func TestEvictionPreconditionsPreventDeletion(t *testing.T) {
	t.Run("unverified record", func(t *testing.T) {
		env := newLifecycleTestEnv(t, CachePresent, true)
		ctx, cancel := persistenceContext()
		record, err := env.engine.repository.get(ctx, lifecycleTestHash)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		record.VerifiedAt = nil
		if changed, err := env.engine.repository.replace(context.Background(), record, StateReplicated); err != nil || !changed {
			t.Fatalf("clear verification: changed=%t err=%v", changed, err)
		}
		if _, err := env.engine.Evict(context.Background(), lifecycleTestHash); err == nil {
			t.Fatal("Evict() accepted a record without persisted verification")
		}
		assertLifecyclePayloadExists(t, env)
	})

	t.Run("destination missing", func(t *testing.T) {
		env := newLifecycleTestEnv(t, CachePresent, true)
		env.runner.destinationErr = &BlockedError{Reason: "destination payload is missing"}
		queued, err := env.engine.Evict(context.Background(), lifecycleTestHash)
		if err != nil {
			t.Fatal(err)
		}
		if err := env.engine.runCacheOperation(context.Background(), queued); err != nil {
			t.Fatal(err)
		}
		if env.api.count("stop") != 0 {
			t.Fatal("torrent was stopped before destination availability was confirmed")
		}
		assertLifecyclePayloadExists(t, env)
	})

	t.Run("qBittorrent stop failure", func(t *testing.T) {
		env := newLifecycleTestEnv(t, CachePresent, true)
		env.api.stopFailure = true
		queued, err := env.engine.Evict(context.Background(), lifecycleTestHash)
		if err != nil {
			t.Fatal(err)
		}
		if err := env.engine.runCacheOperation(context.Background(), queued); err != nil {
			t.Fatal(err)
		}
		if env.api.count("stop") != 1 {
			t.Fatalf("stop calls = %d, want one failed stop", env.api.count("stop"))
		}
		assertLifecyclePayloadExists(t, env)
	})
}

func TestRestoreVerifiesRechecksThenResumesOriginalTorrent(t *testing.T) {
	env := newLifecycleTestEnv(t, CacheEvicted, false)
	queued, err := env.engine.Restore(context.Background(), lifecycleTestHash)
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if queued.CacheState != CacheEvicted || queued.CacheOperation != CacheOperationRestore {
		t.Fatalf("Restore() = %+v, want queued operation", queued)
	}
	if err := env.engine.runCacheOperation(context.Background(), queued); err != nil {
		t.Fatalf("restore and verify: %v", err)
	}
	intermediate, err := env.engine.Get(context.Background(), lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if intermediate.CacheState != CachePresent || intermediate.CacheOperation != CacheOperationSeed || intermediate.RestoredAt == nil {
		t.Fatalf("post-recheck record = %+v, want present with queued seed resume", intermediate)
	}
	if env.api.count("start") != 0 {
		t.Fatal("torrent started before cache state was persisted as present")
	}
	if err := env.engine.runCacheOperation(context.Background(), intermediate); err != nil {
		t.Fatalf("resume seeding: %v", err)
	}
	final, err := env.engine.Get(context.Background(), lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if final.CacheState != CachePresent || final.CacheOperation != "" || final.CacheError != "" {
		t.Fatalf("final restoration record = %+v", final)
	}
	selected := filepath.Join(env.sourceRoot, "media", "payload.bin")
	if contents, err := os.ReadFile(selected); err != nil || string(contents) != "payload" {
		t.Fatalf("restored file = %q, err %v", contents, err)
	}
	restoreIndex := env.api.index("restore")
	verifyIndex := env.api.index("verify")
	recheckIndex := env.api.index("recheck")
	startIndex := env.api.index("start")
	if !(restoreIndex >= 0 && restoreIndex < verifyIndex && verifyIndex < recheckIndex && recheckIndex < startIndex) {
		t.Fatalf("restore event order = %v, want restore, verify, recheck, start", env.api.events())
	}
}

func TestRestoreRecheckFailureNeverStartsSeeding(t *testing.T) {
	env := newLifecycleTestEnv(t, CacheEvicted, false)
	env.api.recheckFailure = true
	queued, err := env.engine.Restore(context.Background(), lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if err := env.engine.runCacheOperation(context.Background(), queued); err != nil {
		t.Fatal(err)
	}
	record, err := env.engine.Get(context.Background(), lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if record.CacheState != CacheRestoring || record.CacheOperation != CacheOperationRestore || record.CacheError == "" {
		t.Fatalf("failed recheck state = %+v, want incomplete restoring operation", record)
	}
	if env.api.count("start") != 0 {
		t.Fatal("torrent resumed before a successful recheck")
	}
}

func TestRestorePreconditionsAndCollisionsPreserveSourceFiles(t *testing.T) {
	t.Run("destination missing", func(t *testing.T) {
		env := newLifecycleTestEnv(t, CacheEvicted, false)
		env.runner.destinationErr = &BlockedError{Reason: "destination payload is missing"}
		queued, err := env.engine.Restore(context.Background(), lifecycleTestHash)
		if err != nil {
			t.Fatal(err)
		}
		if err := env.engine.runCacheOperation(context.Background(), queued); err != nil {
			t.Fatal(err)
		}
		if env.api.count("stop") != 0 || env.api.count("recheck") != 0 || env.api.count("start") != 0 {
			t.Fatalf("restoration proceeded before destination check: %v", env.api.events())
		}
		if _, err := os.Lstat(filepath.Join(env.sourceRoot, "media", "payload.bin")); !os.IsNotExist(err) {
			t.Fatalf("missing source path was unexpectedly created: %v", err)
		}
	})

	t.Run("mismatching existing file", func(t *testing.T) {
		env := newLifecycleTestEnv(t, CacheEvicted, false)
		path := filepath.Join(env.sourceRoot, "media", "payload.bin")
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("changed"), 0o640); err != nil {
			t.Fatal(err)
		}
		queued, err := env.engine.Restore(context.Background(), lifecycleTestHash)
		if err != nil {
			t.Fatal(err)
		}
		if err := env.engine.runCacheOperation(context.Background(), queued); err != nil {
			t.Fatal(err)
		}
		if contents, err := os.ReadFile(path); err != nil || string(contents) != "changed" {
			t.Fatalf("mismatching source file was overwritten: contents=%q err=%v", contents, err)
		}
		if env.api.count("recheck") != 0 || env.api.count("start") != 0 {
			t.Fatalf("restore collision continued into recheck/seeding: %v", env.api.events())
		}
	})
}

func TestInterruptedRestorationReusesIdenticalExistingPayload(t *testing.T) {
	env := newLifecycleTestEnv(t, CacheRestoring, true)
	ctx, cancel := persistenceContext()
	record, err := env.engine.repository.get(ctx, lifecycleTestHash)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if err := env.engine.runCacheOperation(context.Background(), record); err != nil {
		t.Fatalf("recover restoration: %v", err)
	}
	updated, err := env.engine.Get(context.Background(), lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if updated.CacheState != CachePresent || updated.CacheOperation != CacheOperationSeed {
		t.Fatalf("recovered restore record = %+v, want present with seed operation", updated)
	}
	contents, err := os.ReadFile(filepath.Join(env.sourceRoot, "media", "payload.bin"))
	if err != nil || string(contents) != "payload" {
		t.Fatalf("existing restored file changed: contents=%q err=%v", contents, err)
	}
}

func TestSeedResumeFailureCanBeRetriedWithRestore(t *testing.T) {
	env := newLifecycleTestEnv(t, CachePresent, true)
	env.api.startFailure = true
	ctx, cancel := persistenceContext()
	record, err := env.engine.repository.get(ctx, lifecycleTestHash)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	queued := record
	queued.CacheOperation = CacheOperationSeed
	queued.CacheUpdatedAt = time.Now().UTC()
	ctx, cancel = persistenceContext()
	changed, err := env.engine.repository.replaceCache(ctx, record, queued)
	cancel()
	if err != nil || !changed {
		t.Fatalf("queue seed operation: changed=%t err=%v", changed, err)
	}
	if err := env.engine.runCacheOperation(context.Background(), queued); err != nil {
		t.Fatal(err)
	}
	failed, err := env.engine.Get(context.Background(), lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if failed.CacheState != CachePresent || failed.CacheOperation != CacheOperationSeed || failed.CacheError == "" {
		t.Fatalf("failed seed record = %+v", failed)
	}
	env.api.startFailure = false
	requeued, err := env.engine.Restore(context.Background(), lifecycleTestHash)
	if err != nil {
		t.Fatalf("Restore() did not allow seed retry: %v", err)
	}
	if err := env.engine.runCacheOperation(context.Background(), requeued); err != nil {
		t.Fatal(err)
	}
	final, err := env.engine.Get(context.Background(), lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if final.CacheOperation != "" || final.CacheError != "" || env.api.count("start") != 2 {
		t.Fatalf("seed retry did not clear operation: record=%+v starts=%d", final, env.api.count("start"))
	}
}

func TestInterruptedEvictionRecoveryFinishesPartialDeletion(t *testing.T) {
	env := newLifecycleTestEnv(t, CacheEvicting, true)
	if err := os.Remove(filepath.Join(env.sourceRoot, "media", "payload.bin")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := persistenceContext()
	record, err := env.engine.repository.get(ctx, lifecycleTestHash)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if err := env.engine.runCacheOperation(context.Background(), record); err != nil {
		t.Fatalf("recover eviction: %v", err)
	}
	recovered, err := env.engine.Get(context.Background(), lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.CacheState != CacheEvicted || recovered.CacheOperation != "" {
		t.Fatalf("recovered cache state = %+v, want evicted", recovered)
	}
	if _, err := os.Lstat(filepath.Join(env.sourceRoot, "media", "keep.txt")); err != nil {
		t.Fatalf("unselected source file was removed during recovery: %v", err)
	}
}

func TestLifecycleGuardStopsExternalStartButSkipsIntentionalRecheck(t *testing.T) {
	env := newLifecycleTestEnv(t, CacheRestoring, true)
	env.api.setState("checkingUP", false)
	env.engine.mu.Lock()
	env.engine.activeCacheHash = lifecycleTestHash
	env.engine.activeCacheOperation = CacheOperationRestore
	env.engine.activeCacheChecking = true
	env.engine.mu.Unlock()
	env.engine.reconcileEvictedTorrents(context.Background(), []qbittorrent.Torrent{env.api.currentTorrent()})
	if env.api.count("stop") != 0 {
		t.Fatal("intentional recheck was interrupted by the evicted-torrent guard")
	}

	env.engine.mu.Lock()
	env.engine.activeCacheChecking = false
	env.engine.mu.Unlock()
	env.api.setState("stalledUP", false)
	env.engine.reconcileEvictedTorrents(context.Background(), []qbittorrent.Torrent{env.api.currentTorrent()})
	if env.api.count("stop") != 1 {
		t.Fatalf("unexpectedly running evicted torrent stop calls = %d, want one", env.api.count("stop"))
	}
}

func TestSeedRecoveryWaitsForCheckingResumeData(t *testing.T) {
	env := newLifecycleTestEnv(t, CachePresent, true)
	ctx, cancel := persistenceContext()
	record, err := env.engine.repository.get(ctx, lifecycleTestHash)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	updated := record
	updated.CacheOperation = CacheOperationSeed
	updated.CacheUpdatedAt = time.Now().UTC()
	ctx, cancel = persistenceContext()
	changed, err := env.engine.repository.replaceCache(ctx, record, updated)
	cancel()
	if err != nil || !changed {
		t.Fatalf("queue seed recovery: changed=%t err=%v", changed, err)
	}
	env.api.setResumeChecks(2)
	started := time.Now()
	if err := env.engine.runCacheOperation(context.Background(), updated); err != nil {
		t.Fatalf("resume after startup check: %v", err)
	}
	if time.Since(started) < 250*time.Millisecond || env.api.count("start") != 1 {
		t.Fatalf("startup check was not awaited: elapsed=%s start calls=%d", time.Since(started), env.api.count("start"))
	}
}

func assertLifecyclePayloadExists(t *testing.T, env *lifecycleTestEnv) {
	t.Helper()
	if contents, err := os.ReadFile(filepath.Join(env.sourceRoot, "media", "payload.bin")); err != nil || string(contents) != "payload" {
		t.Fatalf("source payload changed: contents=%q err=%v", contents, err)
	}
}

func newLifecycleTestEnv(t *testing.T, state CacheState, createSource bool) *lifecycleTestEnv {
	t.Helper()
	root := t.TempDir()
	if createSource {
		if err := os.MkdirAll(filepath.Join(root, "media"), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "media", "payload.bin"), []byte("payload"), 0o640); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "media", "keep.txt"), []byte("keep"), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	api := &lifecycleAPI{
		calls: make(map[string]int),
		torrent: qbittorrent.Torrent{
			Hash:         lifecycleTestHash,
			Name:         "Example",
			State:        "stalledUP",
			Progress:     1,
			Size:         7,
			TotalSize:    11,
			AmountLeft:   0,
			Ratio:        1.5,
			SeedingTime:  100,
			SavePath:     "/downloads",
			DownloadPath: "/downloads",
			ContentPath:  "/downloads/media",
		},
		files: []qbittorrent.File{
			{Index: 0, Name: "media/payload.bin", Size: 7, Priority: 1, Progress: 1},
			{Index: 1, Name: "media/keep.txt", Size: 4, Priority: 0, Progress: 1},
		},
		stopped: state == CacheEvicted || state == CacheEvicting || state == CacheRestoring,
	}
	if state == CacheEvicted || state == CacheEvicting || state == CacheRestoring {
		api.torrent.State = "missingFiles"
		api.torrent.Progress = 0
		api.torrent.Size = 0
		api.torrent.AmountLeft = 7
		api.files[0].Progress = 0
	}
	server := httptest.NewServer(api)
	t.Cleanup(server.Close)
	client, err := qbittorrent.New(server.URL, "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "seedcache.db"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	source := config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: root}
	destination := config.DestinationConfig{SSHTarget: "testhost", BasePath: "/destination"}
	engine, err := NewEngine(db, client, source, destination, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Files: []File{{Path: "media/payload.bin", Size: 7, Index: 0}}, TotalSize: 7}
	now := time.Now().UTC()
	verifiedAt := now.Add(-time.Hour)
	operation := CacheOperation("")
	if state == CacheEvicting {
		operation = CacheOperationEvict
	} else if state == CacheRestoring {
		operation = CacheOperationRestore
	}
	record := Record{
		Hash:                lifecycleTestHash,
		State:               StateReplicated,
		Manifest:            manifest,
		ManifestFingerprint: manifest.Fingerprint(),
		SourceIdentity:      engine.sourceIdentity(),
		DestinationIdentity: engine.destinationIdentity(),
		CreatedAt:           now,
		UpdatedAt:           now,
		VerifiedAt:          &verifiedAt,
		CacheState:          state,
		CacheOperation:      operation,
		CacheUpdatedAt:      now,
	}
	if state == CachePresent {
		record.CacheState = CachePresent
	}
	if state == CacheEvicted {
		record.EvictedAt = &verifiedAt
	}
	if _, inserted, err := engine.repository.createIfAbsent(context.Background(), record); err != nil || !inserted {
		t.Fatalf("create lifecycle record: inserted=%t err=%v", inserted, err)
	}
	runner := &lifecycleTestRunner{api: api, sourceRoot: root}
	engine.runner = runner
	return &lifecycleTestEnv{engine: engine, client: client, sourceRoot: root, api: api, runner: runner}
}

type lifecycleTestEnv struct {
	engine     *Engine
	client     *qbittorrent.Client
	sourceRoot string
	api        *lifecycleAPI
	runner     *lifecycleTestRunner
}

type lifecycleAPI struct {
	mu               sync.Mutex
	torrent          qbittorrent.Torrent
	files            []qbittorrent.File
	stopped          bool
	stopFailure      bool
	startFailure     bool
	recheckFailure   bool
	recheckPolls     int
	resumeCheckPolls int
	calls            map[string]int
	eventList        []string
}

func (api *lifecycleAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/v2/auth/login":
		http.SetCookie(w, &http.Cookie{Name: "SID", Value: "lifecycle"})
		_, _ = w.Write([]byte("Ok."))
	case "/api/v2/torrents/info":
		api.serveInfo(w, r)
	case "/api/v2/torrents/files":
		api.mu.Lock()
		defer api.mu.Unlock()
		if r.URL.Query().Get("hash") != api.torrent.Hash {
			_, _ = w.Write([]byte("[]"))
			return
		}
		writeTestJSON(w, api.files)
	case "/api/v2/torrents/stop":
		api.mu.Lock()
		api.calls["stop"]++
		api.eventList = append(api.eventList, "stop")
		fail := api.stopFailure
		if !fail {
			api.stopped = true
			if api.torrent.State != "missingFiles" {
				api.torrent.State = "pausedUP"
			}
		}
		api.mu.Unlock()
		if fail {
			http.Error(w, "stop failed", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("Ok."))
	case "/api/v2/torrents/start":
		api.mu.Lock()
		api.calls["start"]++
		api.eventList = append(api.eventList, "start")
		fail := api.startFailure
		if !fail {
			api.stopped = false
			api.torrent.State = "stalledUP"
		}
		api.mu.Unlock()
		if fail {
			http.Error(w, "start failed", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("Ok."))
	case "/api/v2/torrents/recheck":
		api.mu.Lock()
		api.calls["recheck"]++
		api.eventList = append(api.eventList, "recheck")
		api.recheckPolls = 2
		api.torrent.State = "checkingUP"
		api.mu.Unlock()
		_, _ = w.Write([]byte("Ok."))
	default:
		http.NotFound(w, r)
	}
}

func (api *lifecycleAPI) serveInfo(w http.ResponseWriter, r *http.Request) {
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.torrent.State == "checkingResumeData" && api.resumeCheckPolls > 0 {
		api.resumeCheckPolls--
		if api.resumeCheckPolls == 0 {
			api.torrent.State = "stalledUP"
		}
	}
	if api.torrent.State == "checkingUP" && api.recheckPolls > 0 {
		api.recheckPolls--
		if api.recheckPolls == 0 {
			if api.recheckFailure {
				api.torrent.State = "missingFiles"
				api.torrent.Progress = 0
				api.torrent.Size = 0
				api.torrent.AmountLeft = 7
				api.files[0].Progress = 0
			} else {
				api.torrent.State = "pausedUP"
				api.torrent.Progress = 1
				api.torrent.Size = 7
				api.torrent.AmountLeft = 0
				api.files[0].Progress = 1
			}
		}
	}
	filter := r.URL.Query().Get("filter")
	hash := r.URL.Query().Get("hashes")
	if filter == "checking" {
		if strings.Contains(strings.ToLower(api.torrent.State), "checking") && (hash == "" || hash == api.torrent.Hash) {
			writeTestJSON(w, []qbittorrent.Torrent{api.torrent})
		} else {
			_, _ = w.Write([]byte("[]"))
		}
		return
	}
	if filter == "stopped" {
		if !api.stopped || (hash != "" && hash != api.torrent.Hash) {
			_, _ = w.Write([]byte("[]"))
			return
		}
		writeTestJSON(w, []qbittorrent.Torrent{api.torrent})
		return
	}
	if hash != "" && hash != api.torrent.Hash {
		_, _ = w.Write([]byte("[]"))
		return
	}
	writeTestJSON(w, []qbittorrent.Torrent{api.torrent})
}

func (api *lifecycleAPI) count(action string) int {
	api.mu.Lock()
	defer api.mu.Unlock()
	return api.calls[action]
}

func (api *lifecycleAPI) index(event string) int {
	api.mu.Lock()
	defer api.mu.Unlock()
	for index, current := range api.eventList {
		if current == event {
			return index
		}
	}
	return -1
}

func (api *lifecycleAPI) events() []string {
	api.mu.Lock()
	defer api.mu.Unlock()
	return append([]string(nil), api.eventList...)
}

func (api *lifecycleAPI) setState(state string, stopped bool) {
	api.mu.Lock()
	api.torrent.State = state
	api.stopped = stopped
	api.mu.Unlock()
}

func (api *lifecycleAPI) setResumeChecks(count int) {
	api.mu.Lock()
	api.torrent.State = "checkingResumeData"
	api.resumeCheckPolls = count
	api.mu.Unlock()
}

func (api *lifecycleAPI) currentTorrent() qbittorrent.Torrent {
	api.mu.Lock()
	defer api.mu.Unlock()
	return api.torrent
}

type lifecycleTestRunner struct {
	api               *lifecycleAPI
	sourceRoot        string
	destinationErr    error
	verifyRestoredErr error
}

func (r *lifecycleTestRunner) Transfer(context.Context, Manifest) error { return nil }

func (r *lifecycleTestRunner) Verify(context.Context, Manifest) error { return nil }

func (r *lifecycleTestRunner) DestinationAvailable(context.Context, Manifest) error {
	r.api.mu.Lock()
	r.api.eventList = append(r.api.eventList, "destination")
	r.api.mu.Unlock()
	return r.destinationErr
}

func (r *lifecycleTestRunner) Restore(ctx context.Context, manifest Manifest) error {
	r.api.mu.Lock()
	r.api.eventList = append(r.api.eventList, "restore")
	r.api.mu.Unlock()
	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := filepath.Join(r.sourceRoot, filepath.FromSlash(file.Path))
		contents := lifecycleRestoredContents(file)
		if existing, err := os.ReadFile(path); err == nil {
			if !bytes.Equal(existing, contents) {
				return &BlockedError{Reason: "a source payload path already contains different data"}
			}
			continue
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return err
		}
		if err := os.WriteFile(path, contents, 0o640); err != nil {
			return err
		}
	}
	return nil
}

func (r *lifecycleTestRunner) VerifyRestored(_ context.Context, manifest Manifest) error {
	r.api.mu.Lock()
	r.api.eventList = append(r.api.eventList, "verify")
	r.api.mu.Unlock()
	if r.verifyRestoredErr != nil {
		return r.verifyRestoredErr
	}
	for _, file := range manifest.Files {
		contents, err := os.ReadFile(filepath.Join(r.sourceRoot, filepath.FromSlash(file.Path)))
		if err != nil {
			return fmt.Errorf("read restored test file %q: %w", file.Path, err)
		}
		if len(contents) != int(file.Size) || !bytes.Equal(contents, lifecycleRestoredContents(file)) {
			return fmt.Errorf("restored test file %q did not verify", file.Path)
		}
	}
	return nil
}

func lifecycleRestoredContents(file File) []byte {
	if filepath.Base(file.Path) == "payload.bin" && file.Size == int64(len("payload")) {
		return []byte("payload")
	}
	return bytes.Repeat([]byte("x"), int(file.Size))
}
