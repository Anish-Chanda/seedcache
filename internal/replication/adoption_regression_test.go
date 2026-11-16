package replication

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anish-chanda/seedcache/internal/qbittorrent"
)

type adoptionControlRunner struct {
	runner
	available func(context.Context, Manifest) error
	transfer  func(context.Context, Manifest) error
}

func (r *adoptionControlRunner) DestinationAvailable(ctx context.Context, m Manifest) error {
	return r.available(ctx, m)
}
func (r *adoptionControlRunner) Transfer(ctx context.Context, m Manifest) error {
	return r.transfer(ctx, m)
}

type bulkAdoptionOutcome struct {
	report AdoptionReport
	err    error
}

func adoptionReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for adoption test barrier")
		var zero T
		return zero
	}
}

func adoptionStartWorker(t *testing.T, e *Engine) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var err error
	go func() { err = e.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		adoptionReceive(t, done)
		if err != nil {
			t.Errorf("worker failed: %v", err)
		}
	})
}

func adoptionPendingBatch(t *testing.T) (*schedulerTestEnv, string, string) {
	t.Helper()
	a, b := strings.Repeat("a", 40), strings.Repeat("b", 40)
	env := newSchedulerTestEnv(t, []testTorrent{singleFileTorrent(a, "batch-a.bin", "payload"), singleFileTorrent(b, "batch-b.bin", "payload")})
	if err := env.engine.Reconcile(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	for _, hash := range []string{a, b} {
		if _, err := env.engine.Queue(context.Background(), hash); err != nil {
			t.Fatal(err)
		}
	}
	return env, a, b
}

func adoptionAssertReservations(t *testing.T, e *Engine, want int) {
	t.Helper()
	if err := e.acquireCatalog(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.releaseCatalog()
	if len(e.adoptionReservations) != want {
		t.Fatalf("reservations=%v, want %d hashes", e.adoptionReservations, want)
	}
}

func TestBulkAdoptionReservesWholeBatchAndAllowsUnrelatedReplication(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		t.Run(fmt.Sprintf("dry_run=%t", dryRun), func(t *testing.T) {
			env, a, b := adoptionPendingBatch(t)
			enteredA, enteredB := make(chan struct{}), make(chan struct{})
			releaseA, releaseB := make(chan struct{}), make(chan struct{})
			transfers := make(chan string, 8)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			env.engine.runner = &adoptionControlRunner{runner: &fakeRunner{}, available: func(ctx context.Context, m Manifest) error {
				var release <-chan struct{}
				switch m.Files[0].Path {
				case "batch-a.bin":
					close(enteredA)
					release = releaseA
				case "batch-b.bin":
					close(enteredB)
					release = releaseB
				default:
					return nil
				}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}, transfer: func(ctx context.Context, m Manifest) error { transfers <- m.Files[0].Path; return nil }}
			outcome := make(chan bulkAdoptionOutcome, 1)
			go func() { r, err := env.engine.AdoptAll(ctx, dryRun); outcome <- bulkAdoptionOutcome{r, err} }()
			adoptionReceive(t, enteredA)
			adoptionAssertReservations(t, env.engine, 2)
			for _, hash := range []string{a, b} {
				if _, err := env.engine.Retry(context.Background(), hash); !errors.Is(err, ErrConflict) {
					t.Fatalf("reserved retry: %v", err)
				}
				if _, err := env.engine.Queue(context.Background(), hash); !errors.Is(err, ErrConflict) {
					t.Fatalf("reserved queue: %v", err)
				}
			}
			outside := strings.Repeat("c", 40)
			env.setTorrent(singleFileTorrent(outside, "outside-one.bin", "payload"))
			if _, err := env.engine.Queue(context.Background(), outside); err != nil {
				t.Fatal(err)
			}
			adoptionStartWorker(t, env.engine)
			if path := adoptionReceive(t, transfers); path != "outside-one.bin" {
				t.Fatalf("worker copied reserved batch path %q", path)
			}
			waitForRecord(t, env.engine, outside, func(r Record) bool { return r.State == StateReplicated })
			close(releaseA)
			adoptionReceive(t, enteredB)
			outside2 := strings.Repeat("d", 40)
			env.setTorrent(singleFileTorrent(outside2, "outside-two.bin", "payload"))
			if _, err := env.engine.Queue(context.Background(), outside2); err != nil {
				t.Fatal(err)
			}
			if path := adoptionReceive(t, transfers); path != "outside-two.bin" {
				t.Fatalf("worker claimed a different batch torrent between adoptions: %q", path)
			}
			waitForRecord(t, env.engine, outside2, func(r Record) bool { return r.State == StateReplicated })
			record, err := env.engine.Get(context.Background(), b)
			if err != nil || record.State != StatePending || record.Attempts != 0 {
				t.Fatalf("pending batch record changed: %+v %v", record, err)
			}
			close(releaseB)
			result := adoptionReceive(t, outcome)
			if result.err != nil {
				t.Fatal(result.err)
			}
			adoptionAssertReservations(t, env.engine, 0)
			if dryRun {
				if result.report.Summary.WouldAdopt != 2 {
					t.Fatalf("preview=%+v", result.report)
				}
				for _, hash := range []string{a, b} {
					waitForRecord(t, env.engine, hash, func(r Record) bool { return r.State == StateReplicated && r.Attempts == 1 })
				}
			} else {
				if result.report.Summary.Adopted != 2 {
					t.Fatalf("report=%+v", result.report)
				}
				for _, hash := range []string{a, b} {
					r, err := env.engine.Get(context.Background(), hash)
					if err != nil || r.State != StateAdopted || r.Attempts != 0 || r.VerifiedAt != nil {
						t.Fatalf("adoption raced replication: %+v %v", r, err)
					}
				}
			}
		})
	}
}

func TestBulkAdoptionReportsExistingTransferAndPreservesItsContext(t *testing.T) {
	for _, failFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("active_fails=%t", failFirst), func(t *testing.T) {
			env, a, _ := adoptionPendingBatch(t)
			transferStarted := make(chan string, 8)
			activeRelease := make(chan struct{})
			enteredB, releaseB := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			env.engine.runner = &adoptionControlRunner{runner: &fakeRunner{}, available: func(ctx context.Context, m Manifest) error {
				if m.Files[0].Path != "batch-b.bin" {
					return fmt.Errorf("active transfer was inspected for adoption")
				}
				close(enteredB)
				select {
				case <-releaseB:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}, transfer: func(ctx context.Context, m Manifest) error {
				path := m.Files[0].Path
				transferStarted <- path
				if path == "batch-a.bin" && calls.Add(1) == 1 {
					select {
					case <-activeRelease:
						if failFirst {
							return errors.New("temporary copy failure")
						}
						return nil
					case <-ctx.Done():
						return fmt.Errorf("active transfer cancelled: %w", ctx.Err())
					}
				}
				return nil
			}}
			adoptionStartWorker(t, env.engine)
			if path := adoptionReceive(t, transferStarted); path != "batch-a.bin" {
				t.Fatalf("first transfer=%q", path)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			outcome := make(chan bulkAdoptionOutcome, 1)
			go func() { r, err := env.engine.AdoptAll(ctx, false); outcome <- bulkAdoptionOutcome{r, err} }()
			adoptionReceive(t, enteredB)
			close(activeRelease)
			want := StateReplicated
			if failFirst {
				want = StateFailed
			}
			current := waitForRecord(t, env.engine, a, func(r Record) bool { return r.State == want })
			if current.Attempts != 1 {
				t.Fatalf("active transfer was restarted: %+v", current)
			}
			if failFirst && current.LastError != "temporary copy failure" {
				t.Fatalf("active operation was cancelled instead of failing normally: %+v", current)
			}
			if failFirst {
				if _, err := env.engine.repository.db.Exec("UPDATE replications SET next_attempt_at = ? WHERE hash = ?", formatTime(time.Now().Add(-time.Second)), a); err != nil {
					t.Fatal(err)
				}
			}
			outside := strings.Repeat("c", 40)
			env.setTorrent(singleFileTorrent(outside, "outside.bin", "payload"))
			if _, err := env.engine.Queue(context.Background(), outside); err != nil {
				t.Fatal(err)
			}
			if path := adoptionReceive(t, transferStarted); path != "outside.bin" {
				t.Fatalf("active batch transfer retried while reserved: %q", path)
			}
			waitForRecord(t, env.engine, outside, func(r Record) bool { return r.State == StateReplicated })
			close(releaseB)
			result := adoptionReceive(t, outcome)
			if result.err != nil || result.report.Summary.Blocked != 1 || result.report.Summary.Adopted != 1 || result.report.Results[0].Status != "blocked" {
				t.Fatalf("active operation not reported: %+v %v", result.report, result.err)
			}
			adoptionAssertReservations(t, env.engine, 0)
			if failFirst {
				if path := adoptionReceive(t, transferStarted); path != "batch-a.bin" {
					t.Fatalf("retry after reservation release=%q", path)
				}
				waitForRecord(t, env.engine, a, func(r Record) bool { return r.State == StateReplicated && r.Attempts == 2 })
			}
		})
	}
}

func TestBulkAdoptionCancellationReleasesEveryReservation(t *testing.T) {
	env, a, b := adoptionPendingBatch(t)
	entered := make(chan struct{})
	transfers := make(chan string, 4)
	env.engine.runner = &adoptionControlRunner{runner: &fakeRunner{}, available: func(ctx context.Context, m Manifest) error { close(entered); <-ctx.Done(); return ctx.Err() }, transfer: func(ctx context.Context, m Manifest) error { transfers <- m.Files[0].Path; return nil }}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	outcome := make(chan bulkAdoptionOutcome, 1)
	go func() { r, err := env.engine.AdoptAll(ctx, false); outcome <- bulkAdoptionOutcome{r, err} }()
	adoptionReceive(t, entered)
	adoptionAssertReservations(t, env.engine, 2)
	adoptionStartWorker(t, env.engine)
	cancel()
	result := adoptionReceive(t, outcome)
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("cancellation=%v", result.err)
	}
	adoptionAssertReservations(t, env.engine, 0)
	for _, hash := range []string{a, b} {
		waitForRecord(t, env.engine, hash, func(r Record) bool { return r.State == StateReplicated && r.Attempts == 1 })
	}
}

func TestOverlappingBulkReservationsRemainUntilBothOwnersRelease(t *testing.T) {
	env, a, b := adoptionPendingBatch(t)
	if err := env.engine.acquireCatalog(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, err := env.engine.reserveAdoptions(context.Background(), []string{a, b}, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := env.engine.reserveAdoptions(context.Background(), []string{b}, nil)
	env.engine.releaseCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(second.conflicts[b], ErrConflict) {
		t.Fatal("overlapping adoption was not rejected")
	}
	env.engine.releaseAdoptions(first)
	adoptionAssertReservations(t, env.engine, 1)
	if err := env.engine.acquireCatalog(context.Background()); err != nil {
		t.Fatal(err)
	}
	hashes := env.engine.reservedAdoptionHashes()
	env.engine.releaseCatalog()
	if len(hashes) != 1 || hashes[0] != b {
		t.Fatalf("remaining reservation=%v", hashes)
	}
	env.engine.releaseAdoptions(second)
	adoptionAssertReservations(t, env.engine, 0)
}

func TestAdoptionLocalCompletenessUsesSelectedProgress(t *testing.T) {
	for _, kind := range []string{"absent", "complete", "partial files", "preallocated", "partially downloaded", "verified complete", "verified incomplete"} {
		t.Run(kind, func(t *testing.T) {
			present := kind == "complete" || strings.HasPrefix(kind, "verified")
			var env *lifecycleTestEnv
			if strings.HasPrefix(kind, "verified") {
				env = newLifecycleTestEnv(t, CachePresent, true)
			} else {
				env = adoptionEnv(t, present)
			}
			switch kind {
			case "partial files":
				env.api.files[0].Progress = 1
				env.api.files = append(env.api.files, qbittorrent.File{Index: 2, Name: "media/another.bin", Priority: 1, Size: 3, Progress: 0})
				writePayload(t, env.sourceRoot, "media/payload.bin", []byte("payload"))
			case "preallocated", "partially downloaded":
				path := filepath.Join(env.sourceRoot, "media/payload.bin")
				if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
					t.Fatal(err)
				}
				file, err := os.Create(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := file.Truncate(7); err != nil {
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
				env.api.files[0].Progress = 0
				if kind == "partially downloaded" {
					env.api.files[0].Progress = 0.5
				}
			case "verified incomplete":
				env.api.files[0].Progress = 0.5
			}
			before, priorErr := env.engine.Get(context.Background(), lifecycleTestHash)
			result, err := env.engine.Adopt(context.Background(), lifecycleTestHash, false)
			switch kind {
			case "absent":
				if err != nil || result.Record.CacheState != CacheEvicted || result.Record.VerifiedAt != nil {
					t.Fatalf("absent adoption=%+v %v", result, err)
				}
			case "complete":
				if err != nil || result.Record.CacheState != CachePresent || result.Record.VerifiedAt != nil {
					t.Fatalf("complete adoption=%+v %v", result, err)
				}
			case "verified complete":
				if err != nil || result.Status != "already_verified" {
					t.Fatalf("verified adoption=%+v %v", result, err)
				}
			default:
				if err == nil {
					t.Fatal("incomplete local data accepted as present")
				}
			}
			after, afterErr := env.engine.Get(context.Background(), lifecycleTestHash)
			if strings.HasPrefix(kind, "verified") {
				if priorErr != nil || afterErr != nil || after.State != before.State || after.CacheState != before.CacheState || !after.VerifiedAt.Equal(*before.VerifiedAt) {
					t.Fatal("existing verification record changed")
				}
			} else if err != nil && !errors.Is(afterErr, ErrNotFound) {
				t.Fatalf("rejected adoption persisted: %+v %v", after, afterErr)
			}
		})
	}
}

func TestAdoptionRechecksLocalProgressAfterRemoteInspection(t *testing.T) {
	env := adoptionEnv(t, true)
	env.engine.runner = &adoptionRunner{runner: env.runner, available: func(context.Context, Manifest) error {
		env.api.mu.Lock()
		env.api.files[0].Progress = 0.5
		env.api.mu.Unlock()
		return nil
	}}
	if _, err := env.engine.Adopt(context.Background(), lifecycleTestHash, false); err == nil {
		t.Fatal("local progress changed during adoption without rejection")
	}
	if _, err := env.engine.Get(context.Background(), lifecycleTestHash); !errors.Is(err, ErrNotFound) {
		t.Fatal("incomplete snapshot persisted")
	}
}

func TestBulkAdoptionReportsExistingLifecycleOperations(t *testing.T) {
	for _, cache := range []CacheState{CacheEvicting, CacheRestoring} {
		t.Run(string(cache), func(t *testing.T) {
			env := newLifecycleTestEnv(t, cache, false)
			before, err := env.engine.Get(context.Background(), lifecycleTestHash)
			if err != nil {
				t.Fatal(err)
			}
			report, err := env.engine.AdoptAll(context.Background(), false)
			if err != nil || report.Summary.Blocked != 1 || len(report.Results) != 1 || report.Results[0].Error == "" {
				t.Fatalf("active lifecycle report: %+v %v", report, err)
			}
			after, err := env.engine.Get(context.Background(), lifecycleTestHash)
			if err != nil || after.CacheState != before.CacheState || after.CacheOperation != before.CacheOperation || !after.VerifiedAt.Equal(*before.VerifiedAt) {
				t.Fatalf("active lifecycle changed: %+v %v", after, err)
			}
			adoptionAssertReservations(t, env.engine, 0)
		})
	}
}

func TestReservedQueuesAndRetryTimersAreExcluded(t *testing.T) {
	env, a, b := adoptionPendingBatch(t)
	ctx := context.Background()
	if err := env.engine.acquireCatalog(ctx); err != nil {
		t.Fatal(err)
	}
	reservation, err := env.engine.reserveAdoptions(ctx, []string{a, b}, nil)
	env.engine.releaseCatalog()
	if err != nil {
		t.Fatal(err)
	}
	defer env.engine.releaseAdoptions(reservation)
	if _, found, err := env.engine.repository.nextReady(ctx, time.Now(), a, b); err != nil || found {
		t.Fatalf("reserved queue became ready: %v %v", found, err)
	}
	if due, err := env.engine.repository.nextDue(ctx, time.Now(), a, b); err != nil || due != nil {
		t.Fatalf("reserved queue kept worker retry timer due: %v %v", due, err)
	}
	record, err := env.engine.Get(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	record.CacheOperation = CacheOperationRestore
	record.CacheUpdatedAt = time.Now().UTC()
	current, err := env.engine.Get(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := env.engine.repository.replaceCache(ctx, current, record); err != nil || !changed {
		t.Fatalf("cache queue setup: %v %v", changed, err)
	}
	if _, found, err := env.engine.repository.nextCacheOperation(ctx, a, b); err != nil || found {
		t.Fatalf("reserved lifecycle transfer was eligible: %v %v", found, err)
	}
	if _, err := env.engine.Restore(ctx, a); !errors.Is(err, ErrConflict) {
		t.Fatalf("restore bypassed reservation: %v", err)
	}
	if _, err := env.engine.Evict(ctx, a); !errors.Is(err, ErrConflict) {
		t.Fatalf("eviction bypassed reservation: %v", err)
	}
}

func TestBulkSnapshotRemembersTransferThatFinishesDuringCatalogFetch(t *testing.T) {
	env, a, b := adoptionPendingBatch(t)
	catalogEntered, resumeCatalog := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/torrents/info" && r.URL.Query().Get("hashes") == "" {
			close(catalogEntered)
			select {
			case <-resumeCatalog:
			case <-r.Context().Done():
				return
			}
		}
		env.handleQBittorrent(w, r)
	}))
	t.Cleanup(server.Close)
	client, err := qbittorrent.New(server.URL, "user", "password")
	if err != nil {
		t.Fatal(err)
	}
	env.engine.client = client
	activeStarted, activeRelease := make(chan struct{}), make(chan struct{})
	env.engine.runner = &adoptionControlRunner{runner: &fakeRunner{}, available: func(context.Context, Manifest) error { return nil }, transfer: func(ctx context.Context, m Manifest) error {
		if m.Files[0].Path == "batch-a.bin" {
			close(activeStarted)
			select {
			case <-activeRelease:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return fmt.Errorf("worker claimed batch-b before its adoption reservation")
	}}
	adoptionStartWorker(t, env.engine)
	adoptionReceive(t, activeStarted)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	outcome := make(chan bulkAdoptionOutcome, 1)
	go func() { r, err := env.engine.AdoptAll(ctx, false); outcome <- bulkAdoptionOutcome{r, err} }()
	adoptionReceive(t, catalogEntered)
	close(activeRelease)
	waitForRecord(t, env.engine, a, func(r Record) bool { return r.State == StateReplicated })
	// The active marker must also clear before releasing the catalog response.
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		env.engine.mu.Lock()
		active := env.engine.activeHash
		env.engine.mu.Unlock()
		if active == "" {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("finished transfer did not release active marker")
		}
	}
	close(resumeCatalog)
	result := adoptionReceive(t, outcome)
	if result.err != nil || result.report.Summary.Blocked != 1 || result.report.Summary.Adopted != 1 || result.report.Results[0].Status != "blocked" {
		t.Fatalf("initial active transfer was lost from snapshot: %+v %v", result.report, result.err)
	}
	record, err := env.engine.Get(context.Background(), b)
	if err != nil || record.State != StateAdopted || record.Attempts != 0 {
		t.Fatalf("catalog fetch raced worker claim: %+v %v", record, err)
	}
}
