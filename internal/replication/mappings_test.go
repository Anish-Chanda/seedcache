package replication

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anish-chanda/seedcache/internal/config"
)

func localSSH(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync is not installed")
	}
	bin := t.TempDir()
	script := `#!/bin/sh
while [ "$#" -gt 0 ]; do
 case "$1" in -o*) shift ;; *) shift; break ;; esac
done
exec /bin/sh -c "$*"
`
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestMappedRsyncRoundTrip(t *testing.T) {
	localSSH(t)
	root, remote := t.TempDir(), t.TempDir()
	runner := NewRunner(config.SourceConfig{QBittorrentBasePath: "/data", LocalBasePath: root}, config.DestinationConfig{SSHTarget: "storage", BasePath: remote, PathMappings: map[string]string{"movies": "Movies", "tv": "TV Shows"}})
	names := []string{"movies/电影/file ' $`[x]\n.mkv", "tv/Example/episode.mkv"}
	manifest := Manifest{}
	for i, name := range names {
		data := []byte("payload-" + name)
		writePayload(t, root, name, data)
		manifest.Files = append(manifest.Files, File{Path: name, Size: int64(len(data)), Index: i})
		manifest.TotalSize += int64(len(data))
	}
	writePayload(t, root, "movies/unselected.bin", []byte("unselected"))
	writePayload(t, remote, "unrelated.bin", []byte("untouched"))
	ctx := context.Background()
	if err := runner.Transfer(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	if err := runner.Verify(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	if err := runner.DestinationAvailable(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	for _, file := range manifest.Files {
		target := strings.Replace(file.Path, "movies/", "Movies/", 1)
		target = strings.Replace(target, "tv/", "TV Shows/", 1)
		data, err := os.ReadFile(filepath.Join(remote, target))
		if err != nil || string(data) != "payload-"+file.Path {
			t.Fatalf("mapped copy: %q, %v", data, err)
		}
		if err := os.Remove(filepath.Join(root, file.Path)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(remote, "Movies", "unselected.bin")); !os.IsNotExist(err) {
		t.Fatalf("unselected path copied: %v", err)
	}
	if err := os.Remove(filepath.Join(root, "tv/Example")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "tv")); err != nil {
		t.Fatal(err)
	}
	if err := runner.Restore(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	if err := runner.VerifyRestored(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	if err := runner.Restore(ctx, manifest); err != nil {
		t.Fatalf("identical restored files: %v", err)
	}
	for _, file := range manifest.Files {
		data, err := os.ReadFile(filepath.Join(root, file.Path))
		if err != nil || string(data) != "payload-"+file.Path {
			t.Fatalf("restore path: %q, %v", data, err)
		}
	}
	if data, err := os.ReadFile(filepath.Join(remote, "unrelated.bin")); err != nil || string(data) != "untouched" {
		t.Fatal("unrelated remote payload changed")
	}
	// Equal-sized foreign content must survive both copy directions.
	file := manifest.Files[0]
	foreign := strings.Repeat("x", int(file.Size))
	if err := os.WriteFile(filepath.Join(root, file.Path), []byte(foreign), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runner.Restore(ctx, manifest); err == nil {
		t.Fatal("source collision accepted")
	}
	if data, _ := os.ReadFile(filepath.Join(root, file.Path)); string(data) != foreign {
		t.Fatal("source collision overwritten")
	}
	if err := runner.Verify(ctx, manifest); err == nil {
		t.Fatal("checksum mismatch accepted")
	}
}

func TestMappedGroupsBoundariesAndIdentity(t *testing.T) {
	source := config.SourceConfig{QBittorrentBasePath: "/data", LocalBasePath: t.TempDir()}
	dest := config.DestinationConfig{SSHTarget: "storage", BasePath: t.TempDir(), PathMappings: map[string]string{"media/movies": "Movies", "tv": "TV Shows"}}
	r := NewRunner(source, dest)
	for _, name := range []string{"media/movies-other/a", "other/a", "media/movies"} {
		if _, err := r.mappedGroups(Manifest{Files: []File{{Path: name, Size: 1}}, TotalSize: 1}); err == nil {
			t.Errorf("accepted unmatched path %q", name)
		}
	}
	m := Manifest{Files: []File{{Path: "media/movies/Title/file", Size: 1}}, TotalSize: 1}
	groups, err := r.mappedGroups(m)
	if err != nil {
		t.Fatal(err)
	}
	if groups[0].manifest.Files[0].Path != "Title/file" || groups[0].runner.destination.BasePath != filepath.Join(dest.BasePath, "Movies") || m.Files[0].Path != "media/movies/Title/file" {
		t.Fatalf("unexpected mapping: %+v", groups)
	}
	e := &Engine{source: source, destination: dest}
	identity := e.destinationIdentity()
	e.destination.PathMappings = map[string]string{"tv": "TV Shows", "media/movies": "Movies"}
	if e.destinationIdentity() != identity {
		t.Fatal("mapping ordering changed identity")
	}
	e.destination.PathMappings = map[string]string{"tv": "TV Shows", "media/movies": "Films"}
	if e.destinationIdentity() == identity {
		t.Fatal("mapping changes not included in identity")
	}
	r = NewRunner(source, config.DestinationConfig{SSHTarget: "storage", BasePath: dest.BasePath})
	groups, err = r.mappedGroups(m)
	if err != nil || !groups[0].manifest.Equal(m) {
		t.Fatal("identity mapping changed manifest")
	}
}

func TestMappedRsyncRejectsSymlinkedRoots(t *testing.T) {
	localSSH(t)
	local, remote, outside := t.TempDir(), t.TempDir(), t.TempDir()
	writePayload(t, local, "movies/a", []byte("data"))
	if err := os.Symlink(outside, filepath.Join(remote, "Movies")); err != nil {
		t.Fatal(err)
	}
	r := NewRunner(config.SourceConfig{QBittorrentBasePath: "/data", LocalBasePath: local}, config.DestinationConfig{SSHTarget: "storage", BasePath: remote, PathMappings: map[string]string{"movies": "Movies"}})
	m := Manifest{Files: []File{{Path: "movies/a", Size: 4}}, TotalSize: 4}
	if err := r.Transfer(context.Background(), m); err == nil {
		t.Fatal("remote symlink accepted")
	}
	if _, err := os.Stat(filepath.Join(outside, "a")); !os.IsNotExist(err) {
		t.Fatal("remote root escaped")
	}
	if err := os.Remove(filepath.Join(remote, "Movies")); err != nil {
		t.Fatal(err)
	}
	writePayload(t, remote, "Movies/a", []byte("data"))
	if err := os.Remove(filepath.Join(local, "movies/a")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(local, "movies")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(local, "movies")); err != nil {
		t.Fatal(err)
	}
	if err := r.Restore(context.Background(), m); err == nil {
		t.Fatal("local symlink accepted")
	}
}
