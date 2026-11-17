package replication

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/anish-chanda/seedcache/internal/config"
)

// qBittorrent/libtorrent stores bytes from pieces shared with skipped files in
// a .<infohash>.parts file beside the torrent's content directory. These bytes
// are required for seeding even though the skipped files are not selected.
// A part-file is an auxiliary artifact, never part of the selected-file manifest.
const maxPartFileBytes = 1 << 30

func (r *Runner) partFilePath(hash, savePath string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(hash))
	if !validLifecycleHash(normalized) {
		return "", &BlockedError{Reason: "invalid qBittorrent torrent hash"}
	}
	qbRoot, _, err := sourceRoots(r.source)
	if err != nil {
		return "", &BlockedError{Reason: err.Error()}
	}
	cleanSave, err := cleanAbsolutePath(savePath, "qBittorrent save_path")
	if err != nil {
		return "", &BlockedError{Reason: err.Error()}
	}
	relative, ok := relativeToRoot(qbRoot, cleanSave)
	if !ok {
		return "", &BlockedError{Reason: "qBittorrent save_path is outside the configured source root"}
	}
	path := filepath.ToSlash(filepath.Join(relative, "."+normalized+".parts"))
	if _, err := cleanRelativePath(filepath.FromSlash(path), "part-file path"); err != nil {
		return "", &BlockedError{Reason: err.Error()}
	}
	return path, nil
}

func partFileManifest(relative string, size int64) Manifest {
	return Manifest{Files: []File{{Path: relative, Size: size, Index: 0}}, TotalSize: size}
}

// partFileRunner routes an auxiliary file independently of the selected-file
// manifest. Unlike a selected payload, its source path may be directly under
// qBittorrent's save directory, outside any configured path-mapping prefix.
//
// A part-file within a mapped save directory follows that mapping (e.g.
// /data/movies -> Movies). For a save directory above every mapping, a single
// mapping has an unambiguous destination, while multiple mappings use a
// dedicated shared auxiliary directory to avoid arbitrary placement.
func (r *Runner) partFileRunner(hash, savePath string) (*Runner, Manifest, error) {
	relative, err := r.partFilePath(hash, savePath)
	if err != nil {
		return nil, Manifest{}, err
	}
	qbRoot, localRoot, err := sourceRoots(r.source)
	if err != nil {
		return nil, Manifest{}, err
	}
	cleanSave, err := cleanAbsolutePath(savePath, "qBittorrent save_path")
	if err != nil {
		return nil, Manifest{}, err
	}
	saveRel, ok := relativeToRoot(qbRoot, cleanSave)
	if !ok {
		return nil, Manifest{}, &BlockedError{Reason: "qBittorrent save_path is outside the configured source root"}
	}

	remoteDir := filepath.Join(r.destination.BasePath, saveRel)
	if len(r.destination.PathMappings) != 0 {
		if err := config.ValidatePathMappings(r.destination.PathMappings); err != nil {
			return nil, Manifest{}, &BlockedError{Reason: err.Error()}
		}
		keys := make([]string, 0, len(r.destination.PathMappings))
		for key := range r.destination.PathMappings {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return len(keys[i]) > len(keys[j]) })
		matched := false
		for _, key := range keys {
			if saveRel == key || strings.HasPrefix(filepath.ToSlash(saveRel), key+"/") {
				tail := strings.TrimPrefix(strings.TrimPrefix(filepath.ToSlash(saveRel), key), "/")
				remoteDir = filepath.Join(r.destination.BasePath, r.destination.PathMappings[key], filepath.FromSlash(tail))
				matched = true
				break
			}
		}
		if !matched {
			if saveRel == "." && len(keys) == 1 {
				remoteDir = filepath.Join(r.destination.BasePath, r.destination.PathMappings[keys[0]])
			} else {
				remoteDir = filepath.Join(r.destination.BasePath, ".seedcache-partfiles")
			}
		}
	}

	aux := NewRunner(
		config.SourceConfig{
			QBittorrentBasePath: cleanSave,
			LocalBasePath:       filepath.Join(localRoot, saveRel),
		},
		config.DestinationConfig{
			SSHTarget: r.destination.SSHTarget,
			BasePath:  remoteDir,
		},
	)
	return aux, partFileManifest(filepath.Base(relative), 0), nil
}

// BackupPartFile makes a checksum-verified, conflict-safe backup of an existing
// local part-file. It never creates a missing local part-file and never replaces
// a different remote part-file. It must complete before replication is marked
// verified so that future evictions cannot lose indispensable boundary bytes.
func (r *Runner) BackupPartFile(ctx context.Context, hash, savePath string) error {
	aux, manifest, err := r.partFileRunner(hash, savePath)
	if err != nil {
		return err
	}
	local := filepath.Join(aux.source.LocalBasePath, manifest.Files[0].Path)
	if _, err := lstatPathComponents(filepath.Dir(local)); err != nil {
		return err
	}
	info, err := os.Lstat(local)
	if errors.Is(err, os.ErrNotExist) {
		return nil // No shared skipped-file pieces were stored separately.
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > maxPartFileBytes {
		return &BlockedError{Reason: "local qBittorrent part-file is unsafe or too large"}
	}
	manifest.Files[0].Size = info.Size()
	manifest.TotalSize = info.Size()
	if err := aux.transfer(ctx, manifest); err != nil {
		return fmt.Errorf("back up qBittorrent part-file: %w", err)
	}
	if err := aux.verify(ctx, manifest); err != nil {
		return fmt.Errorf("verify qBittorrent part-file backup: %w", err)
	}
	return nil
}

// StagePartFile safely downloads an absent part-file into a private staging
// directory instead of writing to the live filename. Libtorrent loads its
// part-file's piece map when creating the storage object, so neither copying
// over an active part-file nor a force-recheck reloads the map reliably.
// An administrator must stop qBittorrent, move the staged file to its original
// filename, then restart qBittorrent and retry the SeedCache restore.
func (r *Runner) StagePartFile(ctx context.Context, hash, savePath string, selected Manifest) (bool, error) {
	aux, manifest, err := r.partFileRunner(hash, savePath)
	if err != nil {
		return false, err
	}
	local := filepath.Join(aux.source.LocalBasePath, manifest.Files[0].Path)
	if _, err := lstatPathComponents(filepath.Dir(local)); err != nil {
		return false, err
	}
	info, err := os.Lstat(local)
	if err == nil {
		if !info.Mode().IsRegular() || info.Size() > maxPartFileBytes {
			return false, &BlockedError{Reason: "existing qBittorrent part-file is unsafe or too large"}
		}
		return false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}

	size, exists, err := aux.remotePartFileSize(ctx, manifest.Files[0].Path)
	if err != nil || !exists {
		return false, err
	}
	manifest.Files[0].Size = size
	manifest.TotalSize = size

	// Never place a remote .parts file at the live path while qBittorrent
	// is running: it may have an empty cached piece map and can overwrite
	// the newly restored header during shutdown.
	stageDir := filepath.Join(aux.source.LocalBasePath, ".seedcache-staged-partfiles")
	if err := os.Mkdir(stageDir, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
		return false, fmt.Errorf("create part-file staging directory: %w", err)
	}
	if stageInfo, err := os.Lstat(stageDir); err != nil || !stageInfo.IsDir() {
		return false, &BlockedError{Reason: "part-file staging directory is unsafe"}
	}
	stageRunner := *aux
	stageRunner.source.LocalBasePath = stageDir
	if err := stageRunner.restore(ctx, manifest); err != nil {
		return false, fmt.Errorf("stage qBittorrent part-file: %w", err)
	}
	if err := stageRunner.verifyRestored(ctx, manifest); err != nil {
		return false, fmt.Errorf("verify staged qBittorrent part-file: %w", err)
	}
	if len(selected.Files) == 0 {
		return false, errors.New("cannot determine qBittorrent file group without selected payload")
	}
	_, localRoot, err := sourceRoots(r.source)
	if err != nil {
		return false, err
	}
	selectedPath := filepath.Join(localRoot, filepath.FromSlash(selected.Files[0].Path))
	selectedInfo, err := os.Lstat(selectedPath)
	if err != nil || !selectedInfo.Mode().IsRegular() {
		return false, &BlockedError{Reason: "selected payload cannot establish qBittorrent part-file group"}
	}
	stat, ok := selectedInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return false, errors.New("cannot inspect selected payload file group")
	}
	stageFile := filepath.Join(stageDir, manifest.Files[0].Path)
	if err := os.Chown(stageFile, -1, int(stat.Gid)); err != nil {
		return false, fmt.Errorf("set part-file group: %w", err)
	}
	if err := os.Chmod(stageFile, 0o660); err != nil {
		return false, fmt.Errorf("set part-file permissions: %w", err)
	}
	return true, nil
}

func (r *Runner) remotePartFileSize(ctx context.Context, name string) (int64, bool, error) {
	if _, err := cleanRelativePath(name, "part-file name"); err != nil || filepath.Base(name) != name {
		return 0, false, &BlockedError{Reason: "invalid part-file name"}
	}
	_, destinationRoot, target, err := r.paths()
	if err != nil {
		return 0, false, err
	}
	remote := filepath.Join(destinationRoot, name)
	quoted := shellQuote(remote)
	cmd := "if test -L " + quoted + "; then echo UNSAFE; " +
		"elif test -f " + quoted + "; then " +
		"if test -r " + quoted + "; then stat -c %s -- " + quoted + "; else echo UNSAFE; fi; " +
		"elif test -e " + quoted + "; then echo UNSAFE; else echo MISSING; fi"
	sshPath, err := exec.LookPath("ssh")
	if err != nil {
		return 0, false, &BlockedError{Reason: "OpenSSH client is unavailable"}
	}
	output, err := runSSH(ctx, sshPath, target, cmd, nil)
	if err != nil {
		return 0, false, fmt.Errorf("inspect remote qBittorrent part-file: %w", err)
	}
	value := strings.TrimSpace(string(output))
	if value == "MISSING" {
		return 0, false, nil
	}
	if value == "UNSAFE" {
		return 0, false, &BlockedError{Reason: "remote qBittorrent part-file is unsafe or inaccessible"}
	}
	size, err := strconv.ParseInt(value, 10, 64)
	if err != nil || size < 0 || size > maxPartFileBytes {
		return 0, false, &BlockedError{Reason: "remote qBittorrent part-file has an invalid size"}
	}
	return size, true, nil
}
