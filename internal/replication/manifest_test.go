package replication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/anish-chanda/seedcache/internal/config"
	"github.com/anish-chanda/seedcache/internal/qbittorrent"
)

func TestBuildManifestMapsSelectedFilesAndIgnoresExcludedFiles(t *testing.T) {
	localBase := t.TempDir()
	writePayload(t, localBase, "bundle/movie one.mkv", []byte("movie"))
	writePayload(t, localBase, "bundle/sub/episode\n2.mkv", []byte("episode"))
	source := config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: localBase}
	torrent := qbittorrent.Torrent{
		State:       "stoppedUP",
		Progress:    1,
		AmountLeft:  0,
		TotalSize:   500,
		Size:        int64(len("movie") + len("episode")),
		SavePath:    "/downloads",
		ContentPath: "/downloads/bundle",
	}
	files := []qbittorrent.File{
		{Index: 0, Name: "bundle/movie one.mkv", Size: int64(len("movie")), Priority: 1, Progress: 1},
		{Index: 1, Name: "bundle/sub/episode\n2.mkv", Size: int64(len("episode")), Priority: 6, Progress: 1},
		{Index: 2, Name: "../excluded-file", Size: -1, Priority: 0, Progress: 0},
	}

	manifest, err := BuildManifest(torrent, files, source)
	if err != nil {
		t.Fatalf("BuildManifest() error = %v", err)
	}
	want := Manifest{
		Files: []File{
			{Path: "bundle/movie one.mkv", Size: int64(len("movie")), Index: 0},
			{Path: "bundle/sub/episode\n2.mkv", Size: int64(len("episode")), Index: 1},
		},
		TotalSize: int64(len("movie") + len("episode")),
	}
	if !manifest.Equal(want) {
		t.Fatalf("BuildManifest() = %#v, want %#v", manifest, want)
	}
}

func TestBuildManifestAcceptsVerifiedSelectedFilesWithSkippedPieceAccounting(t *testing.T) {
	root := t.TempDir()
	writePayload(t, root, "bundle/video.bin", []byte("video"))
	source := config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: root}
	files := []qbittorrent.File{
		{Index: 0, Name: "bundle/video.bin", Size: 5, Priority: 1, Progress: 1},
		{Index: 1, Name: "bundle/description.txt", Size: 4, Priority: 0, Progress: 1},
	}
	torrent := qbittorrent.Torrent{
		State: "stoppedUP", Progress: 0.999, AmountLeft: 0, Size: 8, TotalSize: 9,
		SavePath: "/downloads", ContentPath: "/downloads/bundle",
	}
	for _, size := range []int64{5, 8, 9} {
		torrent.Size = size
		manifest, err := BuildManifest(torrent, files, source)
		if err != nil {
			t.Fatalf("size=%d: BuildManifest() error = %v", size, err)
		}
		if manifest.TotalSize != 5 || len(manifest.Files) != 1 {
			t.Fatalf("size=%d: manifest = %+v, want one selected 5-byte file", size, manifest)
		}
	}
}

func TestBuildManifestRejectsInvalidPieceSizeAndIncompleteSelection(t *testing.T) {
	root := t.TempDir()
	writePayload(t, root, "bundle/video.bin", []byte("video"))
	source := config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: root}
	files := []qbittorrent.File{
		{Index: 0, Name: "bundle/video.bin", Size: 5, Priority: 1, Progress: 1},
		{Index: 1, Name: "bundle/description.txt", Size: 4, Priority: 0, Progress: 1},
	}
	base := qbittorrent.Torrent{
		State: "stoppedUP", Progress: 0.999, Size: 8, TotalSize: 9,
		SavePath: "/downloads", ContentPath: "/downloads/bundle",
	}
	cases := []struct {
		name    string
		torrent qbittorrent.Torrent
		files   []qbittorrent.File
	}{
		{"reported size below selected bytes", withTorrentSize(base, 4), files},
		{"reported size above total bytes", withTorrentSize(base, 10), files},
		{"missing corroborating total", func() qbittorrent.Torrent { t := base; t.TotalSize = 0; return t }(), files},
		{"mismatched total", func() qbittorrent.Torrent { t := base; t.TotalSize = 8; return t }(), files},
		{"remaining bytes", withAmountLeft(base, 1), files},
		{"selected file incomplete", base, []qbittorrent.File{
			{Index: 0, Name: "bundle/video.bin", Size: 5, Priority: 1, Progress: 0.99}, files[1],
		}},
		{"no skipped files but aggregate incomplete", withTorrentSize(base, 5), files[:1]},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := BuildManifest(tt.torrent, tt.files, source); !errors.Is(err, ErrIneligible) {
				t.Fatalf("BuildManifest() error = %v, want ErrIneligible", err)
			}
		})
	}
}

func TestBuildManifestSupportsSingleFileLayout(t *testing.T) {
	localBase := t.TempDir()
	writePayload(t, localBase, "movies/single file.mkv", []byte("data"))
	torrent := qbittorrent.Torrent{
		State:       "uploading",
		Progress:    1,
		Size:        4,
		SavePath:    "/downloads/movies",
		ContentPath: "/downloads/movies/single file.mkv",
	}
	manifest, err := BuildManifest(torrent, []qbittorrent.File{{
		Index: 0, Name: "single file.mkv", Size: 4, Priority: 1, Progress: 1,
	}}, config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: localBase})
	if err != nil {
		t.Fatalf("BuildManifest() error = %v", err)
	}
	if len(manifest.Files) != 1 || manifest.Files[0].Path != "movies/single file.mkv" {
		t.Fatalf("manifest = %#v, want source-relative single file", manifest)
	}
}

func TestBuildManifestEligibilityAndBlockedPaths(t *testing.T) {
	localBase := t.TempDir()
	writePayload(t, localBase, "dir/file", []byte("data"))
	baseTorrent := qbittorrent.Torrent{
		State:       "stoppedUP",
		Progress:    1,
		Size:        4,
		SavePath:    "/downloads",
		ContentPath: "/downloads/dir/file",
	}
	baseFiles := []qbittorrent.File{{Index: 0, Name: "dir/file", Size: 4, Priority: 1, Progress: 1}}
	source := config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: localBase}

	tests := []struct {
		name        string
		torrent     qbittorrent.Torrent
		files       []qbittorrent.File
		wantBlocked bool
	}{
		{name: "busy state", torrent: withTorrentState(baseTorrent, "checkingUP"), files: baseFiles},
		{name: "downloading state", torrent: withTorrentState(baseTorrent, "stalledDL"), files: baseFiles},
		{name: "incomplete aggregate", torrent: withProgress(baseTorrent, 0.9), files: baseFiles},
		{name: "remaining selected bytes", torrent: withAmountLeft(baseTorrent, 1), files: baseFiles},
		{name: "incomplete selected file", torrent: baseTorrent, files: []qbittorrent.File{{Index: 0, Name: "dir/file", Size: 4, Priority: 1, Progress: 0.9}}},
		{name: "automatic management", torrent: withAutoTMM(baseTorrent), files: baseFiles, wantBlocked: true},
		{name: "missing files state", torrent: withTorrentState(baseTorrent, "missingFiles"), files: baseFiles, wantBlocked: true},
		{name: "traversal", torrent: baseTorrent, files: []qbittorrent.File{{Index: 0, Name: "../file", Size: 4, Priority: 1, Progress: 1}}, wantBlocked: true},
		{name: "content path mismatch", torrent: withContentPath(baseTorrent, "/elsewhere"), files: baseFiles, wantBlocked: true},
		{name: "save path traversal", torrent: withSavePath(baseTorrent, "/downloads/link/../safe"), files: baseFiles, wantBlocked: true},
		{name: "selected size mismatch", torrent: withTorrentSize(baseTorrent, 5), files: baseFiles},
		{name: "missing payload", torrent: withContentPath(baseTorrent, "/downloads/dir/missing"), files: []qbittorrent.File{{Index: 0, Name: "dir/missing", Size: 4, Priority: 1, Progress: 1}}, wantBlocked: true},
		{name: "actual size mismatch", torrent: baseTorrent, files: []qbittorrent.File{{Index: 0, Name: "dir/file", Size: 5, Priority: 1, Progress: 1}}, wantBlocked: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BuildManifest(tt.torrent, tt.files, source)
			if err == nil {
				t.Fatal("BuildManifest() succeeded, want an error")
			}
			var blocked *BlockedError
			if got := errors.As(err, &blocked); got != tt.wantBlocked {
				t.Fatalf("BuildManifest() blocked = %t, want %t (error: %v)", got, tt.wantBlocked, err)
			}
			if !tt.wantBlocked && !errors.Is(err, ErrIneligible) {
				t.Fatalf("BuildManifest() error = %v, want ErrIneligible", err)
			}
		})
	}
}

func TestBuildManifestRejectsSourceSymlinkComponents(t *testing.T) {
	localBase := t.TempDir()
	outside := t.TempDir()
	writePayload(t, outside, "target", []byte("data"))
	if err := os.Symlink(outside, filepath.Join(localBase, "link")); err != nil {
		t.Fatal(err)
	}
	torrent := qbittorrent.Torrent{State: "stoppedUP", Progress: 1, Size: 4, SavePath: "/downloads/link", ContentPath: "/downloads/link/target"}
	files := []qbittorrent.File{{Index: 0, Name: "target", Size: 4, Priority: 1, Progress: 1}}
	_, err := BuildManifest(torrent, files, config.SourceConfig{QBittorrentBasePath: "/downloads", LocalBasePath: localBase})
	var blocked *BlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("BuildManifest() error = %v, want BlockedError", err)
	}
}

func TestManifestEqualAndFingerprintIgnoreFileOrder(t *testing.T) {
	left := Manifest{Files: []File{{Path: "b", Size: 2, Index: 1}, {Path: "a", Size: 1, Index: 0}}, TotalSize: 3}
	right := Manifest{Files: []File{{Path: "a", Size: 1, Index: 0}, {Path: "b", Size: 2, Index: 1}}, TotalSize: 3}
	if !left.Equal(right) {
		t.Fatal("Equal() = false for manifests with the same files in a different order")
	}
	if left.Fingerprint() != right.Fingerprint() {
		t.Fatal("Fingerprint() differs for manifests with the same files in a different order")
	}
	right.Files[0].Size++
	if left.Equal(right) || left.Fingerprint() == right.Fingerprint() {
		t.Fatal("manifest change was not reflected in equality and fingerprint")
	}
}

func withTorrentState(torrent qbittorrent.Torrent, state string) qbittorrent.Torrent {
	torrent.State = state
	return torrent
}

func withAutoTMM(torrent qbittorrent.Torrent) qbittorrent.Torrent {
	torrent.AutoTMM = true
	return torrent
}

func withContentPath(torrent qbittorrent.Torrent, path string) qbittorrent.Torrent {
	torrent.ContentPath = path
	return torrent
}

func withTorrentSize(torrent qbittorrent.Torrent, size int64) qbittorrent.Torrent {
	torrent.Size = size
	return torrent
}

func withProgress(torrent qbittorrent.Torrent, progress float64) qbittorrent.Torrent {
	torrent.Progress = progress
	return torrent
}

func withAmountLeft(torrent qbittorrent.Torrent, amount int64) qbittorrent.Torrent {
	torrent.AmountLeft = amount
	return torrent
}

func withSavePath(torrent qbittorrent.Torrent, path string) qbittorrent.Torrent {
	torrent.SavePath = path
	return torrent
}

func writePayload(t *testing.T, root, relative string, content []byte) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
}
