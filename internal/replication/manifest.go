package replication

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/anish-chanda/seedcache/internal/config"
	"github.com/anish-chanda/seedcache/internal/qbittorrent"
)

type File struct {
	Path  string `json:"path"`
	Size  int64  `json:"size"`
	Index int    `json:"index"`
}

type Manifest struct {
	Files     []File `json:"files"`
	TotalSize int64  `json:"total_size"`
}

// BuildManifest maps qBittorrent's paths into the source mount and verifies
// the selected payload before it can be queued for transfer.
func BuildManifest(torrent qbittorrent.Torrent, files []qbittorrent.File, source config.SourceConfig) (Manifest, error) {
	if torrent.AutoTMM {
		return Manifest{}, &BlockedError{Reason: "disable Automatic Torrent Management for this torrent"}
	}
	if strings.EqualFold(torrent.State, "missingFiles") {
		return Manifest{}, &BlockedError{Reason: "qBittorrent reports missing torrent files"}
	}
	if !readyTorrentState(torrent.State) {
		return Manifest{}, fmt.Errorf("%w: qBittorrent state is %q", ErrIneligible, torrent.State)
	}
	if torrent.Size < 0 || torrent.AmountLeft != 0 || !isFinite(torrent.Progress) || torrent.Progress < 0 || torrent.Progress > 1 {
		return Manifest{}, fmt.Errorf("%w: qBittorrent still reports incomplete selected data", ErrIneligible)
	}
	if len(files) == 0 {
		return Manifest{}, fmt.Errorf("%w: qBittorrent returned no torrent files", ErrIneligible)
	}

	qbBase, localBase, err := sourceRoots(source)
	if err != nil {
		return Manifest{}, &BlockedError{Reason: err.Error()}
	}
	savePath, err := cleanAbsolutePath(torrent.SavePath, "qBittorrent save_path")
	if err != nil {
		return Manifest{}, &BlockedError{Reason: err.Error()}
	}
	contentPath, err := cleanAbsolutePath(torrent.ContentPath, "qBittorrent content_path")
	if err != nil {
		return Manifest{}, &BlockedError{Reason: err.Error()}
	}
	if _, ok := relativeToRoot(qbBase, savePath); !ok {
		return Manifest{}, &BlockedError{Reason: "qBittorrent save_path is outside the configured source root"}
	}
	if _, ok := relativeToRoot(qbBase, contentPath); !ok {
		return Manifest{}, &BlockedError{Reason: "qBittorrent content_path is outside the configured source root"}
	}

	manifest := Manifest{Files: make([]File, 0, len(files))}
	seenIndexes := make(map[int]struct{}, len(files))
	seenPaths := make(map[string]struct{}, len(files))
	for _, file := range files {
		if file.Priority < 0 {
			return Manifest{}, &BlockedError{Reason: fmt.Sprintf("qBittorrent returned an invalid priority for file %d", file.Index)}
		}
		if file.Priority == 0 {
			continue
		}
		if file.Size < 0 {
			return Manifest{}, &BlockedError{Reason: fmt.Sprintf("qBittorrent returned a negative size for file %d", file.Index)}
		}
		if _, exists := seenIndexes[file.Index]; exists {
			return Manifest{}, &BlockedError{Reason: fmt.Sprintf("qBittorrent returned duplicate file index %d", file.Index)}
		}
		seenIndexes[file.Index] = struct{}{}
		if !completeFileProgress(file.Progress, file.Size) {
			return Manifest{}, fmt.Errorf("%w: selected file %q is incomplete", ErrIneligible, file.Name)
		}

		name, err := cleanRelativePath(file.Name, "qBittorrent file name")
		if err != nil {
			return Manifest{}, &BlockedError{Reason: err.Error()}
		}
		qBPath := filepath.Clean(filepath.Join(savePath, name))
		qBRelative, ok := relativeToRoot(qbBase, qBPath)
		if !ok {
			return Manifest{}, &BlockedError{Reason: fmt.Sprintf("selected file %q is outside the configured source root", file.Name)}
		}

		if len(files) == 1 {
			if qBPath != contentPath {
				return Manifest{}, &BlockedError{Reason: "qBittorrent content_path does not match the single-file path"}
			}
		} else {
			if _, ok := relativeToRoot(contentPath, qBPath); !ok || qBPath == contentPath {
				return Manifest{}, &BlockedError{Reason: fmt.Sprintf("selected file %q is outside qBittorrent content_path", file.Name)}
			}
		}

		if err := validateSourceFile(localBase, qBRelative, file.Size); err != nil {
			return Manifest{}, err
		}

		manifestPath := filepath.ToSlash(qBRelative)
		if _, exists := seenPaths[manifestPath]; exists {
			return Manifest{}, &BlockedError{Reason: fmt.Sprintf("selected torrent files collide at %q", manifestPath)}
		}
		seenPaths[manifestPath] = struct{}{}
		if file.Size > math.MaxInt64-manifest.TotalSize {
			return Manifest{}, &BlockedError{Reason: "selected torrent files exceed the supported total size"}
		}
		manifest.Files = append(manifest.Files, File{Path: manifestPath, Size: file.Size, Index: file.Index})
		manifest.TotalSize += file.Size
	}

	if len(manifest.Files) == 0 {
		return Manifest{}, fmt.Errorf("%w: torrent has no selected files", ErrIneligible)
	}
	var allFilesSize int64
	hasSkipped, validAllSizes := false, true
	for _, file := range files {
		if file.Priority == 0 {
			hasSkipped = true
		}
		if file.Size < 0 || file.Size > math.MaxInt64-allFilesSize {
			validAllSizes = false
			continue
		}
		allFilesSize += file.Size
	}
	if !hasSkipped && torrent.Size > 0 && !completeProgress(torrent.Progress) {
		return Manifest{}, fmt.Errorf("%w: qBittorrent still reports incomplete selected data", ErrIneligible)
	}
	if !validAllSizes && torrent.Size != manifest.TotalSize ||
		(validAllSizes && !consistentSelectedSize(torrent, manifest.TotalSize, allFilesSize, hasSkipped)) {
		return Manifest{}, fmt.Errorf("%w: qBittorrent selected size does not match its file list", ErrIneligible)
	}
	sort.Slice(manifest.Files, func(i, j int) bool {
		if manifest.Files[i].Path != manifest.Files[j].Path {
			return manifest.Files[i].Path < manifest.Files[j].Path
		}
		return manifest.Files[i].Index < manifest.Files[j].Index
	})
	return manifest, nil
}

// qBittorrent's size field is piece-accounted for some partially selected
// torrents. Shared pieces may contribute skipped-file bytes to size even when
// the selected payload itself is byte-for-byte complete. The per-selected-file
// progress checks and zero amount_left remain mandatory.
func consistentSelectedSize(torrent qbittorrent.Torrent, selectedBytes, allBytes int64, hasSkipped bool) bool {
	if torrent.Size == selectedBytes {
		return true
	}
	return hasSkipped && torrent.TotalSize == allBytes &&
		selectedBytes < torrent.Size && torrent.Size <= allBytes
}

func (m Manifest) Equal(other Manifest) bool {
	if m.TotalSize != other.TotalSize || len(m.Files) != len(other.Files) {
		return false
	}
	left, right := canonicalFiles(m.Files), canonicalFiles(other.Files)
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (m Manifest) Fingerprint() string {
	canonical := Manifest{Files: canonicalFiles(m.Files), TotalSize: m.TotalSize}
	data, err := json.Marshal(canonical)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func canonicalFiles(files []File) []File {
	result := append([]File(nil), files...)
	sort.Slice(result, func(i, j int) bool {
		if result[i].Path != result[j].Path {
			return result[i].Path < result[j].Path
		}
		if result[i].Index != result[j].Index {
			return result[i].Index < result[j].Index
		}
		return result[i].Size < result[j].Size
	})
	return result
}

func readyTorrentState(state string) bool {
	switch state {
	case "uploading", "stalledUP", "queuedUP", "pausedUP", "stoppedUP", "forcedUP":
		return true
	default:
		return false
	}
}

func completeFileProgress(progress float64, size int64) bool {
	if size == 0 {
		return true
	}
	return completeProgress(progress)
}

func completeProgress(progress float64) bool {
	return !math.IsNaN(progress) && !math.IsInf(progress, 0) && progress == 1
}

func sourceRoots(source config.SourceConfig) (string, string, error) {
	qbBase, err := cleanAbsolutePath(source.QBittorrentBasePath, "source.qbittorrent_base_path")
	if err != nil {
		return "", "", err
	}
	localBase, err := cleanAbsolutePath(source.LocalBasePath, "source.local_base_path")
	if err != nil {
		return "", "", err
	}
	return qbBase, localBase, nil
}

func cleanAbsolutePath(value, label string) (string, error) {
	if strings.TrimSpace(value) == "" || strings.IndexByte(value, 0) >= 0 || !filepath.IsAbs(value) {
		return "", fmt.Errorf("%s must be a non-empty absolute path", label)
	}
	for _, component := range strings.Split(value, string(filepath.Separator)) {
		if component == "." || component == ".." {
			return "", fmt.Errorf("%s contains an unsafe path component", label)
		}
	}
	clean := filepath.Clean(value)
	if clean == string(filepath.Separator) {
		return "", fmt.Errorf("%s must not be the filesystem root", label)
	}
	return clean, nil
}

func cleanRelativePath(value, label string) (string, error) {
	if value == "" || strings.IndexByte(value, 0) >= 0 || filepath.IsAbs(value) {
		return "", fmt.Errorf("%s must be a non-empty relative path", label)
	}
	clean := filepath.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || clean != value {
		return "", fmt.Errorf("%s contains an unsafe path component", label)
	}
	for _, component := range strings.Split(clean, string(filepath.Separator)) {
		if component == ".seedcache-partial" {
			return "", fmt.Errorf("%s uses the reserved .seedcache-partial directory", label)
		}
	}
	return clean, nil
}

func relativeToRoot(root, candidate string) (string, bool) {
	relative, err := filepath.Rel(root, candidate)
	if err != nil || filepath.IsAbs(relative) || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", false
	}
	return relative, true
}

func validateSourceFile(root, relative string, expectedSize int64) error {
	if _, err := lstatPathComponents(root); err != nil {
		return err
	}
	current := root
	components := strings.Split(relative, string(filepath.Separator))
	for index, component := range components {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return &BlockedError{Reason: fmt.Sprintf("source payload %q is missing", filepath.ToSlash(relative))}
			}
			return fmt.Errorf("inspect source payload %q: %w", filepath.ToSlash(relative), err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return &BlockedError{Reason: fmt.Sprintf("source payload %q traverses a symlink", filepath.ToSlash(relative))}
		}
		if index < len(components)-1 {
			if !info.IsDir() {
				return &BlockedError{Reason: fmt.Sprintf("source path component in %q is not a directory", filepath.ToSlash(relative))}
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return &BlockedError{Reason: fmt.Sprintf("source payload %q is not a regular file", filepath.ToSlash(relative))}
		}
		if info.Size() != expectedSize {
			return &BlockedError{Reason: fmt.Sprintf("source payload %q has size %d, expected %d", filepath.ToSlash(relative), info.Size(), expectedSize)}
		}
	}
	return nil
}

func lstatPathComponents(path string) (os.FileInfo, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("source root must be absolute")
	}
	path = filepath.Clean(path)
	current := string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(path, current), current) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, &BlockedError{Reason: fmt.Sprintf("configured source root %q is missing", path)}
			}
			return nil, fmt.Errorf("inspect configured source root %q: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, &BlockedError{Reason: fmt.Sprintf("configured source root %q traverses a symlink", path)}
		}
		if current != path && !info.IsDir() {
			return nil, &BlockedError{Reason: fmt.Sprintf("configured source root component %q is not a directory", current)}
		}
		if current == path {
			if !info.IsDir() {
				return nil, &BlockedError{Reason: fmt.Sprintf("configured source root %q is not a directory", path)}
			}
			return info, nil
		}
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	return info, nil
}
