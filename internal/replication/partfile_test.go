package replication

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anish-chanda/seedcache/internal/config"
)

func TestPartFilePathAndValidation(t *testing.T) {
	root := t.TempDir()
	r := NewRunner(config.SourceConfig{QBittorrentBasePath: "/data", LocalBasePath: root}, config.DestinationConfig{})
	hash := strings.Repeat("a", 40)
	path, err := r.partFilePath(hash, "/data/movies")
	if err != nil || path != "movies/."+hash+".parts" {
		t.Fatalf("path = %q, error = %v", path, err)
	}
	for _, test := range []struct{ hash, save string }{
		{"not-a-hash", "/data/movies"},
		{hash, "/data/movies/.."},
		{hash, "/other/movies"},
		{hash, "/data/movies/../../etc"},
	} {
		if _, err := r.partFilePath(test.hash, test.save); err == nil {
			t.Errorf("unsafe path accepted")
		}
	}
}

func TestPartFileBackupAndStageMapped(t *testing.T) {
	localSSH(t)
	local, remote := t.TempDir(), t.TempDir()
	r := NewRunner(
		config.SourceConfig{QBittorrentBasePath: "/data", LocalBasePath: local},
		config.DestinationConfig{SSHTarget: "storage", BasePath: remote, PathMappings: map[string]string{"movies": "Movies"}},
	)
	hash := strings.Repeat("a", 40)
	partName := "." + hash + ".parts"
	writePayload(t, local, "movies/content/video.bin", []byte("media"))
	writePayload(t, local, "movies/"+partName, []byte("boundary bytes"))
	selected := Manifest{Files: []File{{Path: "movies/content/video.bin", Size: 5, Index: 0}}, TotalSize: 5}

	if err := r.BackupPartFile(context.Background(), hash, "/data/movies"); err != nil {
		t.Fatalf("backup: %v", err)
	}
	remotePath := filepath.Join(remote, "Movies", partName)
	contents, err := os.ReadFile(remotePath)
	if err != nil || string(contents) != "boundary bytes" {
		t.Fatalf("remote part-file missing or incorrect")
	}
	if err := os.Remove(filepath.Join(local, "movies", partName)); err != nil {
		t.Fatal(err)
	}
	staged, err := r.StagePartFile(context.Background(), hash, "/data/movies", selected)
	if err != nil || !staged {
		t.Fatalf("stage: staged=%t, err=%v", staged, err)
	}
	stagePath := filepath.Join(local, "movies", ".seedcache-staged-partfiles", partName)
	contents, err = os.ReadFile(stagePath)
	if err != nil || string(contents) != "boundary bytes" {
		t.Fatalf("staged part-file missing or incorrect")
	}
	if _, err := os.Lstat(filepath.Join(local, "movies", partName)); !os.IsNotExist(err) {
		t.Fatal("staged part-file unexpectedly replaced the live file")
	}
	// Repeated attempts leave the staged copy intact, never populating the
	// live filename behind libtorrent's back.
	staged, err = r.StagePartFile(context.Background(), hash, "/data/movies", selected)
	if err != nil || !staged {
		t.Fatalf("repeated staging: staged=%t, err=%v", staged, err)
	}
	if err := os.Rename(stagePath, filepath.Join(local, "movies", partName)); err != nil {
		t.Fatal(err)
	}
	staged, err = r.StagePartFile(context.Background(), hash, "/data/movies", selected)
	if err != nil || staged {
		t.Fatalf("existing part-file was restaged: staged=%t, err=%v", staged, err)
	}

	// Never overwrite different remote part-file contents when backing up.
	writePayload(t, local, "movies/"+partName, []byte("different data"))
	if err := r.BackupPartFile(context.Background(), hash, "/data/movies"); err == nil {
		t.Fatal("conflicting remote part-file was overwritten")
	}
	contents, err = os.ReadFile(remotePath)
	if err != nil || string(contents) != "boundary bytes" {
		t.Fatal("conflicting backup modified remote part-file")
	}
}

func TestPartFileStageRejectsSymlink(t *testing.T) {
	local := t.TempDir()
	r := NewRunner(config.SourceConfig{QBittorrentBasePath: "/data", LocalBasePath: local}, config.DestinationConfig{})
	hash := strings.Repeat("a", 40)
	if err := os.MkdirAll(filepath.Join(local, "movies"), 0o750); err != nil {
		t.Fatal(err)
	}
	part := filepath.Join(local, "movies", "."+hash+".parts")
	if err := os.Symlink(t.TempDir(), part); err != nil {
		t.Fatal(err)
	}
	_, err := r.StagePartFile(context.Background(), hash, "/data/movies", Manifest{})
	var blocked *BlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("stage symlink error = %v, want BlockedError", err)
	}
}

func TestPartFileBackupAndStageFromMappedRoot(t *testing.T) {
	localSSH(t)
	local, remote := t.TempDir(), t.TempDir()
	r := NewRunner(
		config.SourceConfig{QBittorrentBasePath: "/data", LocalBasePath: local},
		config.DestinationConfig{SSHTarget: "storage", BasePath: remote, PathMappings: map[string]string{"media": "Library"}},
	)
	hash := strings.Repeat("b", 40)
	name := "." + hash + ".parts"
	writePayload(t, local, name, []byte("boundary"))
	writePayload(t, local, "media/payload.bin", []byte("data"))
	selected := Manifest{Files: []File{{Path: "media/payload.bin", Size: 4, Index: 0}}, TotalSize: 4}
	if err := r.BackupPartFile(context.Background(), hash, "/data"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(remote, "Library", name))
	if err != nil || string(got) != "boundary" {
		t.Fatalf("mapped part-file backup: %v", err)
	}
	if err := os.Remove(filepath.Join(local, name)); err != nil {
		t.Fatal(err)
	}
	staged, err := r.StagePartFile(context.Background(), hash, "/data", selected)
	if err != nil || !staged {
		t.Fatalf("mapped stage: staged=%t, err=%v", staged, err)
	}
	got, err = os.ReadFile(filepath.Join(local, ".seedcache-staged-partfiles", name))
	if err != nil || string(got) != "boundary" {
		t.Fatalf("mapped part-file stage: %v", err)
	}
}

func TestPartFileMultiMappingUsesDedicatedStorage(t *testing.T) {
	localSSH(t)
	local, remote := t.TempDir(), t.TempDir()
	r := NewRunner(
		config.SourceConfig{QBittorrentBasePath: "/data", LocalBasePath: local},
		config.DestinationConfig{SSHTarget: "storage", BasePath: remote, PathMappings: map[string]string{"movies": "Movies", "tv": "TV Shows"}},
	)
	hash := strings.Repeat("c", 40)
	name := "." + hash + ".parts"
	writePayload(t, local, name, []byte("boundary"))
	if err := r.BackupPartFile(context.Background(), hash, "/data"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(remote, ".seedcache-partfiles", name))
	if err != nil || string(got) != "boundary" {
		t.Fatalf("dedicated part-file backup: %v", err)
	}
}
