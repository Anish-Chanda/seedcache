package replication

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anish-chanda/seedcache/internal/config"
	"github.com/anish-chanda/seedcache/internal/qbittorrent"
)

func adoptionEnv(t *testing.T, local bool) *lifecycleTestEnv {
	t.Helper()
	state := CacheEvicted
	if local {
		state = CachePresent
	}
	env := newLifecycleTestEnv(t, state, local)
	if _, err := env.engine.repository.db.Exec("DELETE FROM replications"); err != nil {
		t.Fatal(err)
	}
	return env
}

func TestAdoptRemoteOnlyDryRunAndIdempotency(t *testing.T) {
	env := adoptionEnv(t, false)
	ctx := context.Background()
	preview, err := env.engine.Adopt(ctx, lifecycleTestHash, true)
	if err != nil || preview.Status != "would_adopt" || preview.Record.State != StateAdopted || preview.Record.VerifiedAt != nil || preview.Record.CacheState != CacheEvicted {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	if _, err := env.engine.Get(ctx, lifecycleTestHash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("dry run persisted: %v", err)
	}
	actual, err := env.engine.Adopt(ctx, lifecycleTestHash, false)
	if err != nil || actual.Status != "adopted" {
		t.Fatalf("adopt=%+v err=%v", actual, err)
	}
	again, err := env.engine.Adopt(ctx, lifecycleTestHash, false)
	if err != nil || again.Status != "already_adopted" {
		t.Fatalf("repeat=%+v err=%v", again, err)
	}
	if _, err := env.engine.Evict(ctx, lifecycleTestHash); !errors.Is(err, ErrConflict) {
		t.Fatalf("unverified eviction: %v", err)
	}
	for _, action := range []string{"stop", "start", "recheck"} {
		if env.api.count(action) != 0 {
			t.Fatalf("adoption mutated qB: %s", action)
		}
	}
	if err := env.engine.Reconcile(ctx, []qbittorrent.Torrent{env.api.currentTorrent()}); err != nil {
		t.Fatal(err)
	}
	record, err := env.engine.Get(ctx, lifecycleTestHash)
	if err != nil || record.State != StateAdopted || record.VerifiedAt != nil {
		t.Fatalf("discovery changed adoption: %+v %v", record, err)
	}
	if _, found, err := env.engine.repository.nextReady(ctx, time.Now()); err != nil || found {
		t.Fatalf("adoption queued for copy: %v %v", found, err)
	}
	summary, err := env.engine.Summary(ctx)
	if err != nil || summary.Adopted != 1 || summary.Replicated != 0 {
		t.Fatalf("summary=%+v %v", summary, err)
	}
}

func TestAdoptionReplacesOnlyInactiveUnverifiedRecords(t *testing.T) {
	for _, state := range []State{StateBlocked, StatePending, StateFailed, StateTransferring, StateVerifying, StateReplicated} {
		t.Run(string(state), func(t *testing.T) {
			env := newLifecycleTestEnv(t, CacheEvicted, false)
			ctx := context.Background()
			record, err := env.engine.Get(ctx, lifecycleTestHash)
			if err != nil {
				t.Fatal(err)
			}
			record.State = state
			if state != StateReplicated {
				record.VerifiedAt = nil
			}
			if _, err := env.engine.repository.replace(ctx, record, StateReplicated); err != nil {
				t.Fatal(err)
			}
			result, err := env.engine.Adopt(ctx, lifecycleTestHash, false)
			switch state {
			case StateTransferring, StateVerifying:
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("active adopt: %+v %v", result, err)
				}
			case StateReplicated:
				if err != nil || result.Status != "already_verified" || !result.Record.VerifiedAt.Equal(*record.VerifiedAt) {
					t.Fatalf("verified record changed: %+v %v", result, err)
				}
			default:
				if err != nil || result.Status != "adopted" || result.Record.VerifiedAt != nil {
					t.Fatalf("inactive adopt: %+v %v", result, err)
				}
			}
		})
	}
	for _, cache := range []CacheState{CacheEvicting, CacheRestoring} {
		env := newLifecycleTestEnv(t, cache, false)
		if _, err := env.engine.Adopt(context.Background(), lifecycleTestHash, false); !errors.Is(err, ErrConflict) {
			t.Fatalf("cache operation conflict %s: %v", cache, err)
		}
	}
}

func TestAdoptionLocalStateAndInvalidMetadata(t *testing.T) {
	env := adoptionEnv(t, true)
	result, err := env.engine.Adopt(context.Background(), lifecycleTestHash, false)
	if err != nil || result.Record.CacheState != CachePresent || result.Record.VerifiedAt != nil {
		t.Fatalf("present adoption: %+v %v", result, err)
	}
	for _, kind := range []string{"partial", "symlink", "wrong size", "duplicate index", "duplicate path", "traversal", "invalid size", "invalid progress", "autotmm", "running"} {
		t.Run(kind, func(t *testing.T) {
			env := adoptionEnv(t, false)
			switch kind {
			case "partial":
				env.api.files = append(env.api.files, qbittorrent.File{Index: 2, Name: "media/second.bin", Size: 3, Priority: 1})
				writePayload(t, env.sourceRoot, "media/payload.bin", []byte("payload"))
			case "symlink":
				if err := os.MkdirAll(filepath.Join(env.sourceRoot, "media"), 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(t.TempDir(), filepath.Join(env.sourceRoot, "media/payload.bin")); err != nil {
					t.Fatal(err)
				}
			case "wrong size":
				writePayload(t, env.sourceRoot, "media/payload.bin", []byte("short"))
			case "duplicate index":
				env.api.files = append(env.api.files, env.api.files[0])
			case "duplicate path":
				other := env.api.files[0]
				other.Index = 2
				env.api.files = append(env.api.files, other)
			case "traversal":
				env.api.files[0].Name = "../outside"
			case "invalid size":
				env.api.files[0].Size = -1
			case "invalid progress":
				env.api.files[0].Progress = 2
			case "autotmm":
				env.api.torrent.AutoTMM = true
			case "running":
				env.api.stopped = false
			}
			if _, err := env.engine.Adopt(context.Background(), lifecycleTestHash, false); err == nil {
				t.Fatal("unsafe adoption succeeded")
			}
			if _, err := env.engine.Get(context.Background(), lifecycleTestHash); !errors.Is(err, ErrNotFound) {
				t.Fatalf("failed adoption persisted: %v", err)
			}
		})
	}
}

func TestAdoptionRealRemoteAvailabilityIsReadOnly(t *testing.T) {
	localSSH(t)
	for _, kind := range []string{"valid", "missing", "size", "directory", "symlink", "parent symlink"} {
		t.Run(kind, func(t *testing.T) {
			env := adoptionEnv(t, false)
			remote := t.TempDir()
			outside := t.TempDir()
			env.engine.destination = config.DestinationConfig{SSHTarget: "storage", BasePath: remote, PathMappings: map[string]string{"media": "Library"}}
			env.engine.runner = NewRunner(env.engine.source, env.engine.destination)
			writePayload(t, remote, "unrelated/keep.bin", []byte("outside catalog"))
			switch kind {
			case "valid":
				writePayload(t, remote, "Library/payload.bin", []byte("payload"))
			case "size":
				writePayload(t, remote, "Library/payload.bin", []byte("short"))
			case "directory":
				if err := os.MkdirAll(filepath.Join(remote, "Library/payload.bin"), 0o750); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.MkdirAll(filepath.Join(remote, "Library"), 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(remote, "Library/payload.bin")); err != nil {
					t.Fatal(err)
				}
			case "parent symlink":
				if err := os.Symlink(outside, filepath.Join(remote, "Library")); err != nil {
					t.Fatal(err)
				}
			}
			result, err := env.engine.Adopt(context.Background(), lifecycleTestHash, false)
			if (err == nil) != (kind == "valid") {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			data, readErr := os.ReadFile(filepath.Join(remote, "unrelated/keep.bin"))
			if readErr != nil || string(data) != "outside catalog" {
				t.Fatal("unrelated remote file changed")
			}
			if entries, _ := os.ReadDir(outside); len(entries) != 0 {
				t.Fatal("remote symlink escaped root")
			}
		})
	}
}

type adoptionRunner struct {
	runner
	available func(context.Context, Manifest) error
}

func (r *adoptionRunner) DestinationAvailable(ctx context.Context, m Manifest) error {
	return r.available(ctx, m)
}

func TestAdoptionMetadataChangeCancellationAndStorageIdentity(t *testing.T) {
	env := adoptionEnv(t, false)
	env.engine.runner = &adoptionRunner{runner: env.runner, available: func(context.Context, Manifest) error {
		env.api.mu.Lock()
		env.api.files[0].Name = "media/changed.bin"
		env.api.mu.Unlock()
		return nil
	}}
	if _, err := env.engine.Adopt(context.Background(), lifecycleTestHash, false); err == nil {
		t.Fatal("changed metadata accepted")
	}
	env = adoptionEnv(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	env.engine.runner = &adoptionRunner{runner: env.runner, available: func(context.Context, Manifest) error { cancel(); return ctx.Err() }}
	if _, err := env.engine.Adopt(ctx, lifecycleTestHash, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	env.engine.runner = env.runner
	if _, err := env.engine.Adopt(context.Background(), lifecycleTestHash, false); err != nil {
		t.Fatalf("retry adoption: %v", err)
	}
	env.engine.destination.PathMappings = map[string]string{"media": "Library"}
	if _, err := env.engine.Restore(context.Background(), lifecycleTestHash); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed mapping authorized restore: %v", err)
	}
	if _, err := env.engine.Adopt(context.Background(), lifecycleTestHash, false); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed mapping authorized adoption: %v", err)
	}
}

func TestAdoptedRestorationRequiresChecksumAndTorrentRecheck(t *testing.T) {
	for _, kind := range []string{"success", "checksum failure", "recheck failure"} {
		t.Run(kind, func(t *testing.T) {
			env := adoptionEnv(t, false)
			ctx := context.Background()
			if _, err := env.engine.Adopt(ctx, lifecycleTestHash, false); err != nil {
				t.Fatal(err)
			}
			if kind == "checksum failure" {
				env.runner.verifyRestoredErr = fmt.Errorf("checksum failed")
			}
			if kind == "recheck failure" {
				env.api.recheckFailure = true
			}
			queued, err := env.engine.Restore(ctx, lifecycleTestHash)
			if err != nil {
				t.Fatal(err)
			}
			if err := env.engine.runCacheOperation(ctx, queued); err != nil {
				t.Fatal(err)
			}
			current, err := env.engine.Get(ctx, lifecycleTestHash)
			if err != nil {
				t.Fatal(err)
			}
			if kind != "success" {
				if current.State != StateAdopted || current.VerifiedAt != nil || current.CacheState != CacheRestoring || current.CacheError == "" || env.api.count("start") != 0 {
					t.Fatalf("failed restoration verified: %+v", current)
				}
				return
			}
			if current.State != StateReplicated || current.VerifiedAt == nil || current.CacheState != CachePresent || current.CacheOperation != CacheOperationSeed {
				t.Fatalf("successful verification: %+v", current)
			}
			if err := env.engine.runCacheOperation(ctx, current); err != nil {
				t.Fatal(err)
			}
			if env.api.count("start") != 1 || env.api.count("recheck") != 1 {
				t.Fatalf("restore omitted recheck/start: %v", env.api.events())
			}
		})
	}
}

func TestConcurrentAdoptionAndDiscovery(t *testing.T) {
	env := adoptionEnv(t, false)
	entered, release := make(chan struct{}), make(chan struct{})
	env.engine.runner = &adoptionRunner{runner: env.runner, available: func(ctx context.Context, m Manifest) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	done := make(chan error, 1)
	go func() { _, err := env.engine.Adopt(context.Background(), lifecycleTestHash, false); done <- err }()
	<-entered
	reconcile := make(chan error, 1)
	go func() {
		reconcile <- env.engine.Reconcile(context.Background(), []qbittorrent.Torrent{env.api.currentTorrent()})
	}()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-reconcile; err != nil {
		t.Fatal(err)
	}
	record, err := env.engine.Get(context.Background(), lifecycleTestHash)
	if err != nil || record.State != StateAdopted {
		t.Fatalf("reconciliation raced adoption: %+v %v", record, err)
	}
}

func TestBulkAdoptionContinuesAndReportsFailures(t *testing.T) {
	hashes := []string{strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)}
	var items []testTorrent
	for i, hash := range hashes {
		item := singleFileTorrent(hash, fmt.Sprintf("%d.bin", i), "payload")
		items = append(items, item)
	}
	env := newSchedulerTestEnv(t, items)
	env.engine.runner = &adoptionRunner{runner: env.engine.runner, available: func(_ context.Context, m Manifest) error {
		if m.Files[0].Path == "1.bin" {
			return &RemotePayloadError{Path: "1.bin"}
		}
		return nil
	}}
	report, err := env.engine.AdoptAll(context.Background(), true)
	if err != nil || report.Summary.WouldAdopt != 2 || report.Summary.MissingRemote != 1 {
		t.Fatalf("preview=%+v err=%v", report, err)
	}
	records, err := env.engine.List(context.Background())
	if err != nil || len(records) != 0 {
		t.Fatal("bulk dry run modified records")
	}
	report, err = env.engine.AdoptAll(context.Background(), false)
	if err != nil || report.Summary.Adopted != 2 || report.Summary.MissingRemote != 1 || len(report.Results) != 3 {
		t.Fatalf("bulk=%+v err=%v", report, err)
	}
}

func TestAdoptedMappedRestoreAndRestartRecovery(t *testing.T) {
	localSSH(t)
	env := adoptionEnv(t, false)
	ctx := context.Background()
	remote := t.TempDir()
	env.engine.destination = config.DestinationConfig{SSHTarget: "storage", BasePath: remote, PathMappings: map[string]string{"media": "Library"}}
	env.engine.runner = NewRunner(env.engine.source, env.engine.destination)
	writePayload(t, remote, "Library/payload.bin", []byte("payload"))
	writePayload(t, remote, "Library/other.bin", []byte("leave alone"))
	if _, err := env.engine.Adopt(ctx, lifecycleTestHash, false); err != nil {
		t.Fatal(err)
	}
	queued, err := env.engine.Restore(ctx, lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	// Recover after an interrupted transfer has placed an identical selected file.
	queued, err = env.engine.updateCache(queued, func(r *Record) {
		r.CacheState = CacheRestoring
		r.CacheError = "interrupted"
		r.CacheUpdatedAt = time.Now().UTC()
	})
	if err != nil {
		t.Fatal(err)
	}
	writePayload(t, env.sourceRoot, "media/payload.bin", []byte("payload"))
	if err := env.engine.repository.recoverCacheOperations(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	queued, found, err := env.engine.repository.nextCacheOperation(ctx)
	if err != nil || !found {
		t.Fatalf("recover: %+v %v", queued, err)
	}
	if err := env.engine.runCacheOperation(ctx, queued); err != nil {
		t.Fatal(err)
	}
	verified, err := env.engine.Get(ctx, lifecycleTestHash)
	if err != nil || verified.State != StateReplicated || verified.VerifiedAt == nil || verified.CacheOperation != CacheOperationSeed {
		t.Fatalf("restore verification: %+v %v", verified, err)
	}
	if err := env.engine.runCacheOperation(ctx, verified); err != nil {
		t.Fatal(err)
	}
	if env.api.count("start") != 1 || env.api.count("recheck") != 1 {
		t.Fatal("restore skipped qB recheck")
	}
	if env.api.currentTorrent().SavePath != "/downloads" {
		t.Fatal("qB save path changed")
	}
	if data, err := os.ReadFile(filepath.Join(remote, "Library/other.bin")); err != nil || string(data) != "leave alone" {
		t.Fatal("unrelated remote file changed")
	}
}

func TestAdoptionCancellationWhileCatalogBusy(t *testing.T) {
	env := adoptionEnv(t, false)
	if err := env.engine.acquireCatalog(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer env.engine.releaseCatalog()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := env.engine.Adopt(ctx, lifecycleTestHash, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting adoption ignored cancellation: %v", err)
	}
}

func TestAdoptedLocalPayloadIsNeverAutomaticallyEvicted(t *testing.T) {
	env := adoptionEnv(t, true)
	ctx := context.Background()
	if _, err := env.engine.Adopt(ctx, lifecycleTestHash, false); err != nil {
		t.Fatal(err)
	}
	env.engine.ConfigureRetention(config.RetentionConfig{AutoEvict: true, LowFreePercent: 99, TargetFreePercent: 100})
	plan, err := env.engine.EvictionPlan(ctx)
	if err != nil || len(plan.Candidates) != 0 {
		t.Fatalf("unverified adoption is eligible: %+v %v", plan, err)
	}
	if queued, err := env.engine.queueAutomaticEviction(ctx); err != nil || queued {
		t.Fatalf("unverified eviction queued: %v %v", queued, err)
	}
	assertLifecyclePayloadExists(t, env)
}

func TestAdoptedRestorationChecksSpaceBeforeCopying(t *testing.T) {
	env := adoptionEnv(t, false)
	ctx := context.Background()
	env.api.files[0].Size = 1 << 62
	if _, err := env.engine.Adopt(ctx, lifecycleTestHash, false); err != nil {
		t.Fatal(err)
	}
	queued, err := env.engine.Restore(ctx, lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if err := env.engine.runCacheOperation(ctx, queued); err != nil {
		t.Fatal(err)
	}
	record, err := env.engine.Get(ctx, lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if record.VerifiedAt != nil || record.State != StateAdopted || record.CacheError == "" || env.api.index("restore") >= 0 || env.api.count("start") != 0 {
		t.Fatalf("insufficient storage bypassed: %+v", record)
	}
}

func TestAdoptedPresentPayloadRequiresRecheckBeforeVerification(t *testing.T) {
	localSSH(t)
	env := adoptionEnv(t, true)
	remote := t.TempDir()
	ctx := context.Background()
	env.engine.destination = config.DestinationConfig{SSHTarget: "storage", BasePath: remote, PathMappings: map[string]string{"media": "Library"}}
	env.engine.runner = NewRunner(env.engine.source, env.engine.destination)
	writePayload(t, remote, "Library/payload.bin", []byte("payload"))
	result, err := env.engine.Adopt(ctx, lifecycleTestHash, false)
	if err != nil || result.Record.VerifiedAt != nil || result.Record.CacheState != CachePresent {
		t.Fatalf("adopt: %+v %v", result, err)
	}
	queued, err := env.engine.Restore(ctx, lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if err := env.engine.runCacheOperation(ctx, queued); err != nil {
		t.Fatal(err)
	}
	record, err := env.engine.Get(ctx, lifecycleTestHash)
	if err != nil || record.State != StateReplicated || record.VerifiedAt == nil || env.api.count("recheck") != 1 {
		t.Fatalf("verification: %+v %v", record, err)
	}
	assertLifecyclePayloadExists(t, env)
}

func TestBulkAdoptionCancellationKeepsCompletedRecordsRetryable(t *testing.T) {
	hashes := []string{strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)}
	var items []testTorrent
	for i, hash := range hashes {
		items = append(items, singleFileTorrent(hash, fmt.Sprintf("%d.bin", i), "payload"))
	}
	env := newSchedulerTestEnv(t, items)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env.engine.runner = &adoptionRunner{runner: env.engine.runner, available: func(_ context.Context, m Manifest) error {
		if m.Files[0].Path == "1.bin" {
			cancel()
			return ctx.Err()
		}
		return nil
	}}
	report, err := env.engine.AdoptAll(ctx, false)
	if !errors.Is(err, context.Canceled) || report.Summary.Adopted != 1 {
		t.Fatalf("cancelled report: %+v %v", report, err)
	}
	env.engine.runner = &adoptionRunner{runner: env.engine.runner, available: func(context.Context, Manifest) error { return nil }}
	report, err = env.engine.AdoptAll(context.Background(), false)
	if err != nil || report.Summary.AlreadyAdopted != 1 || report.Summary.Adopted != 2 {
		t.Fatalf("retry: %+v %v", report, err)
	}
}

func TestWorkerCannotClaimPendingRecordDuringAdoption(t *testing.T) {
	env := adoptionEnv(t, true)
	ctx := context.Background()
	if err := env.engine.Reconcile(ctx, []qbittorrent.Torrent{env.api.currentTorrent()}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	env.engine.runner = &adoptionRunner{runner: env.runner, available: func(context.Context, Manifest) error { close(entered); <-release; return nil }}
	adoptDone := make(chan error, 1)
	go func() { _, err := env.engine.Adopt(ctx, lifecycleTestHash, false); adoptDone <- err }()
	<-entered
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerDone := make(chan error, 1)
	go func() { workerDone <- env.engine.Run(workerCtx) }()
	close(release)
	if err := <-adoptDone; err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	record, err := env.engine.Get(ctx, lifecycleTestHash)
	if err != nil || record.State != StateAdopted || record.Attempts != 0 {
		t.Fatalf("worker claimed adoption: %+v %v", record, err)
	}
}

func TestAdoptionRefusesKnownPartialPayloads(t *testing.T) {
	for _, partial := range []string{"media/.seedcache-partial/payload.bin", "media/payload.bin.!qB"} {
		t.Run(partial, func(t *testing.T) {
			env := adoptionEnv(t, false)
			writePayload(t, env.sourceRoot, partial, []byte("partial bytes"))
			if _, err := env.engine.Adopt(context.Background(), lifecycleTestHash, false); err == nil {
				t.Fatal("incomplete local transfer adopted as evicted")
			}
			contents, err := os.ReadFile(filepath.Join(env.sourceRoot, partial))
			if err != nil || string(contents) != "partial bytes" {
				t.Fatal("adoption removed partial data")
			}
		})
	}
}

func TestAdoptionPreservesVerifiedRecordWithUnexpectedLocalFiles(t *testing.T) {
	env := newLifecycleTestEnv(t, CachePresent, true)
	ctx := context.Background()
	before, err := env.engine.Get(ctx, lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(env.sourceRoot, "media/payload.bin")); err != nil {
		t.Fatal(err)
	}
	env.api.setState("missingFiles", true)
	if _, err := env.engine.Adopt(ctx, lifecycleTestHash, false); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale cache state accepted: %v", err)
	}
	after, err := env.engine.Get(ctx, lifecycleTestHash)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != before.State || after.CacheState != before.CacheState || !after.VerifiedAt.Equal(*before.VerifiedAt) {
		t.Fatal("adoption reinterpreted existing verified record")
	}
}
