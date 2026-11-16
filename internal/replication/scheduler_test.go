package replication

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anish-chanda/seedcache/internal/config"
	"github.com/anish-chanda/seedcache/internal/qbittorrent"
	"github.com/anish-chanda/seedcache/internal/store"
	"github.com/rs/zerolog"
)

func TestReconcileAndQueueDeduplicate(t *testing.T) {
	env := newSchedulerTestEnv(t, []testTorrent{singleFileTorrent("hash-a", "A.mkv", "payload-a")})
	ctx := context.Background()

	if err := env.engine.Reconcile(ctx, []qbittorrent.Torrent{env.snapshot("hash-a")}); err != nil {
		t.Fatal(err)
	}
	record, err := env.engine.Get(ctx, "HASH-A")
	if err != nil {
		t.Fatal(err)
	}
	if record.State != StatePending || record.Manifest.TotalSize != int64(len("payload-a")) {
		t.Fatalf("reconciled record = %+v, want pending selected-file manifest", record)
	}
	queued, err := env.engine.Queue(ctx, "hash-a")
	if err != nil {
		t.Fatal(err)
	}
	if queued.Hash != record.Hash || queued.State != StatePending {
		t.Fatalf("duplicate Queue() = %+v, want existing pending record", queued)
	}
	list, err := env.engine.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("replication count = %d, want one", len(list))
	}
}

func TestRunProcessesFIFOWithSingleWorkerAndPersistsVerifiedManifest(t *testing.T) {
	env := newSchedulerTestEnv(t, []testTorrent{
		singleFileTorrent("hash-a", "A.mkv", "payload-a"),
		singleFileTorrent("hash-b", "B.mkv", "payload-bb"),
	})
	ctx := context.Background()
	if err := env.engine.Reconcile(ctx, nil); err != nil {
		t.Fatal(err)
	}
	fake := &fakeRunner{transferDelay: 20 * time.Millisecond}
	env.engine.runner = fake
	for _, hash := range []string{"hash-a", "hash-b"} {
		if _, err := env.engine.Queue(ctx, hash); err != nil {
			t.Fatalf("Queue(%s) error = %v", hash, err)
		}
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- env.engine.Run(workerCtx) }()

	first := waitForRecord(t, env.engine, "hash-a", func(record Record) bool { return record.State == StateReplicated })
	second := waitForRecord(t, env.engine, "hash-b", func(record Record) bool { return record.State == StateReplicated })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if fake.maxActive.Load() != 1 {
		t.Errorf("maximum concurrent transfers = %d, want one", fake.maxActive.Load())
	}
	fake.mu.Lock()
	if len(fake.fileOrder) != 2 || fake.fileOrder[0] != "A.mkv" || fake.fileOrder[1] != "B.mkv" {
		t.Errorf("transfer file order = %v, want FIFO [A.mkv B.mkv]", fake.fileOrder)
	}
	fake.mu.Unlock()
	for _, record := range []Record{first, second} {
		if record.VerifiedAt == nil || record.Attempts != 1 || record.ManifestFingerprint != record.Manifest.Fingerprint() {
			t.Errorf("verified record did not persist manifest and verification: %+v", record)
		}
	}
}

func TestRunFailureCanBeExplicitlyRetried(t *testing.T) {
	env := newSchedulerTestEnv(t, []testTorrent{singleFileTorrent("hash-a", "A.mkv", "payload-a")})
	ctx := context.Background()
	if err := env.engine.Reconcile(ctx, []qbittorrent.Torrent{env.snapshot("hash-a")}); err != nil {
		t.Fatal(err)
	}
	fake := &fakeRunner{transferErrors: []error{errors.New("temporary destination outage")}}
	env.engine.runner = fake

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- env.engine.Run(workerCtx) }()
	failed := waitForRecord(t, env.engine, "hash-a", func(record Record) bool { return record.State == StateFailed })
	if failed.NextAttemptAt == nil || failed.LastError != "temporary destination outage" || failed.Attempts != 1 {
		t.Fatalf("failed record = %+v, want scheduled retry after first attempt", failed)
	}

	retried, err := env.engine.Retry(ctx, "hash-a")
	if err != nil {
		t.Fatalf("Retry() error = %v", err)
	}
	if retried.State != StatePending || retried.NextAttemptAt != nil || retried.LastError != "" {
		t.Fatalf("Retry() record = %+v, want clean pending state", retried)
	}
	replicated := waitForRecord(t, env.engine, "hash-a", func(record Record) bool { return record.State == StateReplicated })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if replicated.Attempts != 2 || fake.transferCalls.Load() != 2 {
		t.Errorf("retry attempts = %d, calls = %d; want two", replicated.Attempts, fake.transferCalls.Load())
	}
}

func TestReconcileCancelsActiveTransferWhenManifestChanges(t *testing.T) {
	initial := singleFileTorrent("hash-a", "A.mkv", "payload-a")
	env := newSchedulerTestEnv(t, []testTorrent{initial})
	ctx := context.Background()
	if err := env.engine.Reconcile(ctx, []qbittorrent.Torrent{env.snapshot("hash-a")}); err != nil {
		t.Fatal(err)
	}
	fake := &fakeRunner{transferWaitForCancel: true, started: make(chan struct{}, 1)}
	env.engine.runner = fake
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- env.engine.Run(workerCtx) }()
	fake.waitStarted(t)

	changed := singleFileTorrent("hash-a", "Renamed.mkv", "different payload")
	env.setTorrent(changed)
	if err := env.engine.Reconcile(ctx, []qbittorrent.Torrent{env.snapshot("hash-a")}); err != nil {
		t.Fatal(err)
	}
	blocked := waitForRecord(t, env.engine, "hash-a", func(record Record) bool { return record.State == StateBlocked })
	if !strings.Contains(blocked.LastError, "manifest changed") {
		t.Fatalf("blocked reason = %q, want manifest change", blocked.LastError)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestQueueUnknownTorrentReturnsNotFound(t *testing.T) {
	env := newSchedulerTestEnv(t, nil)
	_, err := env.engine.Queue(context.Background(), "unknown-hash")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Queue() error = %v, want ErrNotFound", err)
	}
}

func TestQBittorrentLookupFailureGatesOtherQueuedTransfers(t *testing.T) {
	env := newSchedulerTestEnv(t, []testTorrent{
		singleFileTorrent("hash-a", "A.mkv", "payload-a"),
		singleFileTorrent("hash-b", "B.mkv", "payload-b"),
	})
	ctx := context.Background()
	if err := env.engine.Reconcile(ctx, nil); err != nil {
		t.Fatal(err)
	}
	for _, hash := range []string{"hash-a", "hash-b"} {
		if _, err := env.engine.Queue(ctx, hash); err != nil {
			t.Fatal(err)
		}
	}
	fake := &fakeRunner{}
	env.engine.runner = fake
	env.setUnavailable(true)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- env.engine.Run(workerCtx) }()

	failed := waitForRecord(t, env.engine, "hash-a", func(record Record) bool { return record.State == StateFailed })
	if failed.NextAttemptAt == nil {
		t.Fatalf("failed record has no retry time: %+v", failed)
	}
	time.Sleep(50 * time.Millisecond)
	records, err := env.engine.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pendingCount := 0
	for _, record := range records {
		if record.State == StatePending {
			pendingCount++
		}
	}
	if fake.transferCalls.Load() != 0 || pendingCount != 1 {
		t.Fatalf("worker ran while qBittorrent unavailable: calls=%d pending=%d", fake.transferCalls.Load(), pendingCount)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

type testTorrent struct {
	torrent qbittorrent.Torrent
	files   []qbittorrent.File
}

func singleFileTorrent(hash, name, contents string) testTorrent {
	return testTorrent{
		torrent: qbittorrent.Torrent{
			Hash:        hash,
			Name:        name,
			State:       "stalledUP",
			Progress:    1,
			Size:        int64(len(contents)),
			TotalSize:   int64(len(contents)),
			AmountLeft:  0,
			SavePath:    "/downloads",
			ContentPath: filepath.Join("/downloads", name),
		},
		files: []qbittorrent.File{{Index: 0, Name: name, Size: int64(len(contents)), Priority: 1, Progress: 1}},
	}
}

type schedulerTestEnv struct {
	engine      *Engine
	server      *httptest.Server
	localRoot   string
	mu          sync.Mutex
	torrents    map[string]testTorrent
	unavailable bool
}

func newSchedulerTestEnv(t *testing.T, torrents []testTorrent) *schedulerTestEnv {
	t.Helper()
	localRoot := t.TempDir()
	env := &schedulerTestEnv{localRoot: localRoot, torrents: make(map[string]testTorrent)}
	for _, item := range torrents {
		env.setTorrent(item)
	}
	env.server = httptest.NewServer(http.HandlerFunc(env.handleQBittorrent))
	t.Cleanup(env.server.Close)
	client, err := qbittorrent.New(env.server.URL, "user", "password")
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "seedcache.db"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	engine, err := NewEngine(db, client,
		config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: localRoot},
		config.DestinationConfig{SSHTarget: "testhost", BasePath: "/storage/torrents"}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	env.engine = engine
	return env
}

func (e *schedulerTestEnv) setTorrent(item testTorrent) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.torrents[strings.ToLower(item.torrent.Hash)] = item
	for _, file := range item.files {
		path := filepath.Join(e.localRoot, file.Name)
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			panic(err)
		}
		contents := strings.Repeat("x", int(file.Size))
		if err := os.WriteFile(path, []byte(contents), 0o640); err != nil {
			panic(err)
		}
	}
}

func (e *schedulerTestEnv) snapshot(hash string) qbittorrent.Torrent {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.torrents[strings.ToLower(hash)].torrent
}

func (e *schedulerTestEnv) setUnavailable(unavailable bool) {
	e.mu.Lock()
	e.unavailable = unavailable
	e.mu.Unlock()
}

func (e *schedulerTestEnv) handleQBittorrent(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/v2/auth/login":
		w.Header().Set("Set-Cookie", "SID=test")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	case "/api/v2/torrents/info":
		e.mu.Lock()
		unavailable := e.unavailable
		item, ok := e.torrents[strings.ToLower(r.URL.Query().Get("hashes"))]
		e.mu.Unlock()
		if unavailable {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		if !ok {
			_, _ = w.Write([]byte("[]"))
			return
		}
		writeTestJSON(w, []qbittorrent.Torrent{item.torrent})
	case "/api/v2/torrents/files":
		e.mu.Lock()
		unavailable := e.unavailable
		item, ok := e.torrents[strings.ToLower(r.URL.Query().Get("hash"))]
		e.mu.Unlock()
		if unavailable {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		if !ok {
			_, _ = w.Write([]byte("[]"))
			return
		}
		writeTestJSON(w, item.files)
	default:
		http.NotFound(w, r)
	}
}

func writeTestJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type fakeRunner struct {
	transferErrors        []error
	transferDelay         time.Duration
	transferWaitForCancel bool
	transferCalls         atomic.Int32
	active                atomic.Int32
	maxActive             atomic.Int32
	mu                    sync.Mutex
	fileOrder             []string
	started               chan struct{}
}

func (r *fakeRunner) Transfer(ctx context.Context, manifest Manifest) error {
	call := int(r.transferCalls.Add(1))
	if len(manifest.Files) > 0 {
		r.mu.Lock()
		r.fileOrder = append(r.fileOrder, manifest.Files[0].Path)
		r.mu.Unlock()
	}
	active := r.active.Add(1)
	for {
		max := r.maxActive.Load()
		if active <= max || r.maxActive.CompareAndSwap(max, active) {
			break
		}
	}
	defer r.active.Add(-1)
	if r.started != nil {
		select {
		case r.started <- struct{}{}:
		default:
		}
	}
	if r.transferWaitForCancel {
		<-ctx.Done()
		return ctx.Err()
	}
	if r.transferDelay > 0 {
		timer := time.NewTimer(r.transferDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	if call <= len(r.transferErrors) {
		return r.transferErrors[call-1]
	}
	return nil
}

func (r *fakeRunner) Verify(context.Context, Manifest) error { return nil }

func (r *fakeRunner) waitStarted(t *testing.T) {
	t.Helper()
	if r.started == nil {
		t.Fatal("fake runner started notification was not configured")
	}
	select {
	case <-r.started:
	case <-time.After(3 * time.Second):
		t.Fatal("transfer did not start")
	}
}

func waitForRecord(t *testing.T, engine *Engine, hash string, predicate func(Record) bool) Record {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		record, err := engine.Get(context.Background(), hash)
		if err == nil && predicate(record) {
			return record
		}
		time.Sleep(10 * time.Millisecond)
	}
	record, err := engine.Get(context.Background(), hash)
	t.Fatalf("record did not reach expected state; last record %+v, error %v", record, err)
	return Record{}
}
