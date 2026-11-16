package replication

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/anish-chanda/seedcache/internal/config"
)

func TestRestorationRefusesInsufficientStorage(t *testing.T) {
	source := config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: t.TempDir()}
	manifest := Manifest{Files: []File{{Path: "huge.bin", Size: math.MaxInt64, Index: 0}}, TotalSize: math.MaxInt64}
	var blocked *BlockedError
	if err := requireSourceSpace(source, manifest); !errors.As(err, &blocked) {
		t.Fatalf("storage check = %v, want blocked", err)
	}
}

func TestRestorationFailuresDoNotRecheckOrStart(t *testing.T) {
	for _, kind := range []string{"destination unavailable", "checksum mismatch", "source collision"} {
		t.Run(kind, func(t *testing.T) {
			env := newLifecycleTestEnv(t, CacheEvicted, false)
			payload := filepath.Join(env.sourceRoot, "media", "payload.bin")
			switch kind {
			case "destination unavailable":
				env.runner.destinationErr = errors.New("SSH destination unavailable")
			case "checksum mismatch":
				env.runner.verifyRestoredErr = &BlockedError{Reason: "checksum mismatch"}
			case "source collision":
				if err := os.MkdirAll(filepath.Dir(payload), 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(payload, []byte("foreign"), 0o640); err != nil {
					t.Fatal(err)
				}
			}
			record, err := env.engine.Restore(context.Background(), lifecycleTestHash)
			if err != nil {
				t.Fatal(err)
			}
			if err := env.engine.runCacheOperation(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			current, err := env.engine.Get(context.Background(), lifecycleTestHash)
			if err != nil {
				t.Fatal(err)
			}
			if current.CacheState == CachePresent || current.CacheError == "" {
				t.Fatalf("failure falsely completed: %+v", current)
			}
			if env.api.count("recheck") != 0 || env.api.count("start") != 0 {
				t.Fatalf("continued after failure: %v", env.api.events())
			}
			if kind == "source collision" {
				data, err := os.ReadFile(payload)
				if err != nil || string(data) != "foreign" {
					t.Fatalf("collision overwritten: %q, %v", data, err)
				}
			}
		})
	}
}

func TestDeletionFailureLeavesRemainingPayloadRecoverable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permissions do not constrain root")
	}
	root := t.TempDir()
	locked := filepath.Join(root, "locked")
	if err := os.Mkdir(locked, 0o750); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first.bin", "locked/second.bin"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("data"), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(locked, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o750) })
	source := config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: root}
	manifest := Manifest{Files: []File{{Path: "first.bin", Size: 4, Index: 0}, {Path: "locked/second.bin", Size: 4, Index: 1}}, TotalSize: 8}
	if err := removeManifestFiles(source, manifest, false); err == nil {
		t.Fatal("deletion falsely succeeded")
	}
	if _, err := os.Stat(filepath.Join(root, "first.bin")); !os.IsNotExist(err) {
		t.Fatalf("first file not removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(locked, "second.bin")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := removeManifestFiles(source, manifest, true); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(locked, "second.bin")); !os.IsNotExist(err) {
		t.Fatalf("second file remains: %v", err)
	}
}
