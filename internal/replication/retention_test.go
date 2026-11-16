package replication

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/anish-chanda/seedcache/internal/config"
	"github.com/anish-chanda/seedcache/internal/qbittorrent"
)

func TestEvictionPlanRequiresBothThresholdsAndOrdersSafeCandidates(t *testing.T) {
	items := []testTorrent{
		retentionTestTorrent("hash-low-ratio", "low-ratio.bin", 1.19, 7200, 10),
		retentionTestTorrent("hash-low-time", "low-time.bin", 1.8, 7199, 20),
		retentionTestTorrent("hash-unverified", "unverified.bin", 1.8, 7200, 30),
		retentionTestTorrent("hash-destination", "destination.bin", 1.8, 7200, 40),
		retentionTestTorrent("hash-shared", "shared.bin", 1.8, 7200, 50),
		retentionTestTorrent("hash-valid-b", "valid-b.bin", 1.8, 7200, 100),
		retentionTestTorrent("hash-valid-a", "valid-a.bin", 1.8, 7200, 100),
		retentionTestTorrent("hash-valid-c", "valid-c.bin", 1.8, 7200, 200),
		retentionTestTorrent("hash-valid-unknown", "valid-unknown.bin", 1.8, 7200, 0),
	}
	owner := retentionTestTorrent("hash-incomplete-owner", "shared.bin", 0.1, 5, 60)
	owner.files[0].Size = items[4].files[0].Size
	owner.torrent.Size = items[4].torrent.Size
	owner.torrent.TotalSize = items[4].torrent.TotalSize
	owner.torrent.State = "downloading"
	owner.torrent.Progress = 0.25
	owner.torrent.AmountLeft = owner.torrent.Size - 1
	owner.files[0].Priority = 0
	owner.files[0].Progress = 0
	items = append(items, owner)

	env := newSchedulerTestEnv(t, items)
	runner := &retentionTestRunner{destinationErrors: make(map[string]error)}
	env.engine.runner = runner
	if err := env.engine.Reconcile(context.Background(), retentionSnapshots(env, items)); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	for _, item := range items {
		if item.torrent.Hash == owner.torrent.Hash {
			continue
		}
		verified := item.torrent.Hash != "hash-unverified"
		record := markRetentionRecord(t, env, item.torrent.Hash, verified)
		if item.torrent.Hash == "hash-destination" {
			runner.destinationErrors[record.Manifest.Fingerprint()] = &BlockedError{Reason: "destination file unavailable"}
		}
	}

	env.engine.ConfigureRetention(config.RetentionConfig{
		AutoEvict:         true,
		MinRatio:          1.2,
		MinSeedingTime:    2 * time.Hour,
		LowFreePercent:    0,
		TargetFreePercent: 100,
	})
	plan, err := env.engine.EvictionPlan(context.Background())
	if err != nil {
		t.Fatalf("EvictionPlan() error = %v", err)
	}
	if !plan.Enabled {
		t.Fatal("EvictionPlan() reported automatic eviction disabled")
	}
	want := []string{"hash-valid-a", "hash-valid-b", "hash-valid-c", "hash-valid-unknown"}
	if len(plan.Candidates) != len(want) {
		t.Fatalf("candidate hashes = %v, want %v", retentionCandidateHashes(plan.Candidates), want)
	}
	for index, hash := range want {
		if plan.Candidates[index].Hash != hash {
			t.Errorf("candidate[%d] = %q, want %q", index, plan.Candidates[index].Hash, hash)
		}
	}
	for _, excluded := range []string{
		"hash-low-ratio",
		"hash-low-time",
		"hash-unverified",
		"hash-destination",
		"hash-shared",
	} {
		for _, candidate := range plan.Candidates {
			if candidate.Hash == excluded {
				t.Errorf("ineligible torrent %q appeared in the eviction plan", excluded)
			}
		}
	}
}

func TestAutomaticEvictionUsesLowAndTargetThresholdHysteresis(t *testing.T) {
	items := []testTorrent{
		retentionTestTorrent("hash-hysteresis-a", "hysteresis-a.bin", 2, 7200, 100),
		retentionTestTorrent("hash-hysteresis-b", "hysteresis-b.bin", 2, 7200, 200),
	}
	env := newSchedulerTestEnv(t, items)
	env.engine.runner = &retentionTestRunner{destinationErrors: make(map[string]error)}
	if err := env.engine.Reconcile(context.Background(), retentionSnapshots(env, items)); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	for _, item := range items {
		markRetentionRecord(t, env, item.torrent.Hash, true)
	}

	env.engine.ConfigureRetention(config.RetentionConfig{})
	space, err := env.engine.EvictionPlan(context.Background())
	if err != nil {
		t.Fatalf("read temporary source filesystem pressure: %v", err)
	}
	free := space.FreePercent
	if free <= 0 || free >= 100 || math.IsNaN(free) || math.IsInf(free, 0) {
		t.Skipf("temporary filesystem free percentage %v cannot represent both threshold transitions", free)
	}
	epsilon := 0.1
	if free < 0.4 {
		epsilon = free / 4
	}
	if 100-free < 0.4 {
		epsilon = (100 - free) / 4
	}
	if epsilon <= 0 {
		t.Skipf("temporary filesystem free percentage %v leaves no threshold margin", free)
	}

	setRetentionWithoutReset(env.engine, free-epsilon, free-epsilon/2)
	plan, err := env.engine.EvictionPlan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Triggered {
		t.Fatalf("pressure triggered at free %.4f%% with low threshold %.4f%%", plan.FreePercent, plan.LowFreePercent)
	}
	if queued, err := env.engine.queueAutomaticEviction(context.Background()); err != nil || queued {
		t.Fatalf("queue above low threshold = (%t, %v), want no operation", queued, err)
	}
	if retentionIsActive(env.engine) {
		t.Fatal("retention latch activated above the low threshold")
	}

	setRetentionWithoutReset(env.engine, free+epsilon, free+2*epsilon)
	plan, err = env.engine.EvictionPlan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Triggered {
		t.Fatalf("pressure did not trigger below low threshold: free=%.4f low=%.4f", plan.FreePercent, plan.LowFreePercent)
	}
	queued, err := env.engine.queueAutomaticEviction(context.Background())
	if err != nil || !queued {
		t.Fatalf("queue below low threshold = (%t, %v), want an operation", queued, err)
	}
	if !retentionIsActive(env.engine) {
		t.Fatal("retention latch was not activated below the low threshold")
	}
	first, err := env.engine.Get(context.Background(), "hash-hysteresis-a")
	if err != nil {
		t.Fatal(err)
	}
	if first.CacheOperation != CacheOperationEvict {
		t.Fatalf("first ordered candidate operation = %q, want eviction", first.CacheOperation)
	}

	setRetentionWithoutReset(env.engine, free-epsilon, free+epsilon)
	plan, err = env.engine.EvictionPlan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Triggered {
		t.Fatalf("pressure unexpectedly triggered above low threshold: free=%.4f low=%.4f", plan.FreePercent, plan.LowFreePercent)
	}
	queued, err = env.engine.queueAutomaticEviction(context.Background())
	if err != nil || !queued {
		t.Fatalf("latched pressure did not queue until target: queued=%t err=%v", queued, err)
	}
	if !retentionIsActive(env.engine) {
		t.Fatal("retention latch cleared before reaching the target threshold")
	}
	second, err := env.engine.Get(context.Background(), "hash-hysteresis-b")
	if err != nil {
		t.Fatal(err)
	}
	if second.CacheOperation != CacheOperationEvict {
		t.Fatalf("second ordered candidate operation = %q, want eviction", second.CacheOperation)
	}

	setRetentionWithoutReset(env.engine, free-2*epsilon, free-epsilon)
	queued, err = env.engine.queueAutomaticEviction(context.Background())
	if err != nil || queued {
		t.Fatalf("queue at or above target = (%t, %v), want no operation", queued, err)
	}
	if retentionIsActive(env.engine) {
		t.Fatal("retention latch remained active after reaching the target threshold")
	}
}

func retentionTestTorrent(hash, name string, ratio float64, seedingSeconds, lastActivity int64) testTorrent {
	item := singleFileTorrent(hash, name, "payload-"+hash)
	item.torrent.Ratio = ratio
	item.torrent.SeedingTime = seedingSeconds
	item.torrent.LastActivity = lastActivity
	return item
}

func retentionSnapshots(env *schedulerTestEnv, items []testTorrent) []qbittorrent.Torrent {
	snapshots := make([]qbittorrent.Torrent, 0, len(items))
	for _, item := range items {
		snapshots = append(snapshots, env.snapshot(item.torrent.Hash))
	}
	return snapshots
}

func markRetentionRecord(t *testing.T, env *schedulerTestEnv, hash string, verified bool) Record {
	t.Helper()
	ctx, cancel := persistenceContext()
	defer cancel()
	record, err := env.engine.repository.get(ctx, hash)
	if err != nil {
		t.Fatalf("get retention test record %q: %v", hash, err)
	}
	if record.State != StatePending {
		t.Fatalf("record %q state = %q, want pending before fixture update", hash, record.State)
	}
	record.State = StateReplicated
	record.UpdatedAt = time.Now().UTC()
	if verified {
		record.VerifiedAt = ptrTime(record.UpdatedAt)
	} else {
		record.VerifiedAt = nil
	}
	changed, err := env.engine.repository.replace(ctx, record, StatePending)
	if err != nil || !changed {
		t.Fatalf("mark record %q replicated: changed=%t err=%v", hash, changed, err)
	}
	return record
}

func retentionCandidateHashes(candidates []EvictionCandidate) []string {
	hashes := make([]string, len(candidates))
	for index, candidate := range candidates {
		hashes[index] = candidate.Hash
	}
	return hashes
}

func setRetentionWithoutReset(engine *Engine, low, target float64) {
	engine.mu.Lock()
	engine.retention.AutoEvict = true
	engine.retention.MinRatio = 1
	engine.retention.MinSeedingTime = 0
	engine.retention.LowFreePercent = low
	engine.retention.TargetFreePercent = target
	engine.mu.Unlock()
}

func retentionIsActive(engine *Engine) bool {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return engine.retentionActive
}

type retentionTestRunner struct {
	destinationErrors map[string]error
}

func (r *retentionTestRunner) Transfer(context.Context, Manifest) error       { return nil }
func (r *retentionTestRunner) Verify(context.Context, Manifest) error         { return nil }
func (r *retentionTestRunner) Restore(context.Context, Manifest) error        { return nil }
func (r *retentionTestRunner) VerifyRestored(context.Context, Manifest) error { return nil }

func (r *retentionTestRunner) DestinationAvailable(_ context.Context, manifest Manifest) error {
	if r.destinationErrors == nil {
		return nil
	}
	return r.destinationErrors[manifest.Fingerprint()]
}

var _ runner = (*retentionTestRunner)(nil)

func TestAutomaticEvictionDefaultsToDisabled(t *testing.T) {
	env := newSchedulerTestEnv(t, nil)
	plan, err := env.engine.EvictionPlan(context.Background())
	if err != nil {
		t.Fatalf("EvictionPlan() error = %v", err)
	}
	if plan.Enabled || plan.Triggered {
		t.Fatalf("default eviction plan = %+v, want disabled without pressure trigger", plan)
	}
	if queued, err := env.engine.queueAutomaticEviction(context.Background()); err != nil || queued {
		t.Fatalf("default queueAutomaticEviction() = (%t, %v), want no operation", queued, err)
	}
}
