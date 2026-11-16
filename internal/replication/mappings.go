package replication

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/anish-chanda/seedcache/internal/config"
	"golang.org/x/sys/unix"
)

type mappedGroup struct {
	runner   *Runner
	manifest Manifest
	prefix   string
}

func (r *Runner) mappedGroups(manifest Manifest) ([]mappedGroup, error) {
	files, _, err := validateManifest(manifest)
	if err != nil {
		return nil, err
	}
	if err := config.ValidatePathMappings(r.destination.PathMappings); err != nil {
		return nil, &BlockedError{Reason: err.Error()}
	}
	if len(r.destination.PathMappings) == 0 {
		return []mappedGroup{{runner: r, manifest: manifest}}, nil
	}
	if _, _, _, err := r.paths(); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(r.destination.PathMappings))
	for key := range r.destination.PathMappings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	groups := make(map[string]*mappedGroup)
	remotePaths := make(map[string]bool, len(files))
	for _, file := range files {
		from := ""
		for _, key := range keys {
			if strings.HasPrefix(file.Path, key+"/") {
				from = key
				break
			}
		}
		if from == "" {
			return nil, &BlockedError{Reason: fmt.Sprintf("selected path %q has no destination mapping", file.Path)}
		}
		remainder := strings.TrimPrefix(file.Path, from+"/")
		remote := r.destination.PathMappings[from] + "/" + remainder
		if remotePaths[remote] {
			return nil, &BlockedError{Reason: "selected files collide at the destination"}
		}
		remotePaths[remote] = true
		group := groups[from]
		if group == nil {
			source := r.source
			source.LocalBasePath = filepath.Join(source.LocalBasePath, filepath.FromSlash(from))
			source.QBittorrentBasePath = filepath.Join(source.QBittorrentBasePath, filepath.FromSlash(from))
			destination := r.destination
			destination.PathMappings = nil
			destination.BasePath = filepath.Join(destination.BasePath, filepath.FromSlash(r.destination.PathMappings[from]))
			group = &mappedGroup{runner: NewRunner(source, destination), prefix: from}
			groups[from] = group
		}
		file.Path = remainder
		group.manifest.Files = append(group.manifest.Files, file)
		group.manifest.TotalSize += file.Size
	}
	result := make([]mappedGroup, 0, len(groups))
	for _, key := range keys {
		if group := groups[key]; group != nil {
			result = append(result, *group)
		}
	}
	return result, nil
}

func (r *Runner) Transfer(ctx context.Context, manifest Manifest) error {
	groups, err := r.mappedGroups(manifest)
	if err != nil {
		return err
	}
	for _, group := range groups {
		if err := group.runner.transfer(ctx, group.manifest); err != nil {
			return err
		}
	}
	return nil
}
func (r *Runner) Verify(ctx context.Context, manifest Manifest) error {
	groups, err := r.mappedGroups(manifest)
	if err != nil {
		return err
	}
	for _, group := range groups {
		if err := group.runner.verify(ctx, group.manifest); err != nil {
			return err
		}
	}
	return nil
}
func (r *Runner) DestinationAvailable(ctx context.Context, manifest Manifest) error {
	groups, err := r.mappedGroups(manifest)
	if err != nil {
		return err
	}
	for _, group := range groups {
		if err := group.runner.destinationAvailable(ctx, group.manifest); err != nil {
			return err
		}
	}
	return nil
}
func (r *Runner) Restore(ctx context.Context, manifest Manifest) error {
	groups, err := r.mappedGroups(manifest)
	if err != nil {
		return err
	}
	if err := r.DestinationAvailable(ctx, manifest); err != nil {
		return err
	}
	for _, group := range groups {
		if group.prefix != "" {
			if err := ensureMappedSourceRoot(r.source.LocalBasePath, group.prefix); err != nil {
				return err
			}
		}
		if err := group.runner.restore(ctx, group.manifest); err != nil {
			return err
		}
	}
	return nil
}
func (r *Runner) VerifyRestored(ctx context.Context, manifest Manifest) error {
	groups, err := r.mappedGroups(manifest)
	if err != nil {
		return err
	}
	for _, group := range groups {
		if err := group.runner.verifyRestored(ctx, group.manifest); err != nil {
			return err
		}
	}
	return nil
}

func ensureMappedSourceRoot(root, prefix string) error {
	fd, err := openAbsoluteDirectoryNoSymlinks(root)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(fd) }()
	for _, part := range strings.Split(prefix, "/") {
		if err := unix.Mkdirat(fd, part, 0o750); err != nil && err != unix.EEXIST {
			return fmt.Errorf("create mapped source directory: %w", err)
		}
		next, err := unix.Openat(fd, part, unix.O_DIRECTORY|unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return &BlockedError{Reason: "mapped source directory is unsafe or inaccessible"}
		}
		_ = unix.Close(fd)
		fd = next
	}
	return nil
}
