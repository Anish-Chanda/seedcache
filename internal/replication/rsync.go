package replication

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/anish-chanda/seedcache/internal/config"
)

const (
	partialDirectory = ".seedcache-partial"
	minimumRsync     = "3.2.3"
	checkTimeout     = 30 * time.Second
	processWaitDelay = 2 * time.Second
)

type Runner struct {
	source      config.SourceConfig
	destination config.DestinationConfig
}

func NewRunner(source config.SourceConfig, destination config.DestinationConfig) *Runner {
	return &Runner{source: source, destination: destination}
}

func (r *Runner) Transfer(ctx context.Context, manifest Manifest) error {
	files, _, err := validateManifest(manifest)
	if err != nil {
		return err
	}
	sourceRoot, destinationRoot, target, err := r.paths()
	if err != nil {
		return err
	}
	if err := validateLocalFiles(sourceRoot, files); err != nil {
		return err
	}
	if err := r.checkCapabilities(ctx); err != nil {
		return err
	}
	if _, err := r.preflight(ctx, target, destinationRoot, files); err != nil {
		return err
	}

	input := fileList(files)
	args := r.rsyncArgs(target, destinationRoot, sourceRoot, false)
	if err := runRsync(ctx, args, input); err != nil {
		return fmt.Errorf("rsync transfer failed: %w", err)
	}
	if err := validateLocalFiles(sourceRoot, files); err != nil {
		return fmt.Errorf("source changed during transfer: %w", err)
	}
	return nil
}

func (r *Runner) Verify(ctx context.Context, manifest Manifest) error {
	files, _, err := validateManifest(manifest)
	if err != nil {
		return err
	}
	sourceRoot, destinationRoot, target, err := r.paths()
	if err != nil {
		return err
	}
	if err := validateLocalFiles(sourceRoot, files); err != nil {
		return err
	}
	if err := r.checkCapabilities(ctx); err != nil {
		return err
	}
	exists, err := r.preflight(ctx, target, destinationRoot, files)
	if err != nil {
		return err
	}
	for _, found := range exists {
		if !found {
			return errors.New("verification failed: a destination file is missing")
		}
	}

	args := r.rsyncArgs(target, destinationRoot, sourceRoot, true)
	output, err := runRsyncOutput(ctx, args, fileList(files))
	if err != nil {
		return fmt.Errorf("rsync checksum verification failed: %w", err)
	}
	changed, err := itemizedFileChanges(output)
	if err != nil {
		return err
	}
	if changed > 0 {
		current, checkErr := r.preflight(ctx, target, destinationRoot, files)
		if checkErr != nil {
			return checkErr
		}
		for _, found := range current {
			if !found {
				return errors.New("verification failed: a destination file disappeared")
			}
		}
		return &BlockedError{Reason: "destination already contains a file with different content"}
	}
	if err := validateLocalFiles(sourceRoot, files); err != nil {
		return fmt.Errorf("source changed during verification: %w", err)
	}
	return nil
}

func (r *Runner) paths() (sourceRoot, destinationRoot, target string, err error) {
	_, sourceRoot, err = sourceRoots(r.source)
	if err != nil {
		return "", "", "", &BlockedError{Reason: err.Error()}
	}
	destinationRoot, err = cleanAbsolutePath(r.destination.BasePath, "destination.base_path")
	if err != nil || destinationRoot == string(filepath.Separator) {
		if err == nil {
			err = errors.New("destination.base_path must not be the filesystem root")
		}
		return "", "", "", &BlockedError{Reason: err.Error()}
	}
	target = strings.TrimSpace(r.destination.SSHTarget)
	if !validSSHTarget(target) {
		return "", "", "", &BlockedError{Reason: "destination.ssh_target must be an SSH host or alias without shell syntax"}
	}
	return sourceRoot, destinationRoot, target, nil
}

func (r *Runner) checkCapabilities(ctx context.Context) error {
	if err := checkRsyncVersion(ctx, "rsync", nil, "local"); err != nil {
		return err
	}
	sshPath, err := exec.LookPath("ssh")
	if err != nil {
		return &BlockedError{Reason: fmt.Sprintf("OpenSSH client is unavailable: %v", err)}
	}
	_, _, target, err := r.paths()
	if err != nil {
		return err
	}
	checkCtx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	output, err := runSSH(checkCtx, sshPath, target, "env -u RSYNC_CHECKSUM_LIST rsync --version", nil)
	if err != nil {
		if sshConfigurationError(err) || missingRemoteRsync(err) || unsupportedRemoteEnv(err) {
			return &BlockedError{Reason: err.Error()}
		}
		return fmt.Errorf("could not check remote rsync: %w", err)
	}
	return checkRsyncVersion(ctx, "remote rsync", output, "remote")
}

func checkRsyncVersion(ctx context.Context, label string, knownOutput []byte, side string) error {
	output := knownOutput
	if output == nil {
		cmd := contextCommand(ctx, label, "--version")
		var err error
		output, err = cmd.CombinedOutput()
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("check %s version: %w", side, ctx.Err())
			}
			return &BlockedError{Reason: fmt.Sprintf("%s executable is unavailable: %v", label, err)}
		}
	}
	version, ok := parseRsyncVersion(output)
	if !ok {
		return &BlockedError{Reason: fmt.Sprintf("could not determine %s rsync version", side)}
	}
	if compareVersions(version, [3]int{3, 2, 3}) < 0 {
		return &BlockedError{Reason: fmt.Sprintf("%s rsync %s is unsupported; version %s or newer is required", side, formatVersion(version), minimumRsync)}
	}
	return nil
}

var (
	rsyncVersionPattern = regexp.MustCompile(`rsync\s+version\s+([0-9]+)\.([0-9]+)\.([0-9]+)`)
	sshTargetPattern    = regexp.MustCompile(`^([a-zA-Z0-9_][a-zA-Z0-9_.-]*@)?[a-zA-Z0-9_][a-zA-Z0-9_.-]*$`)
)

func parseRsyncVersion(output []byte) ([3]int, bool) {
	match := rsyncVersionPattern.FindSubmatch(output)
	if len(match) != 4 {
		return [3]int{}, false
	}
	var version [3]int
	for i := range version {
		value, err := strconv.Atoi(string(match[i+1]))
		if err != nil {
			return [3]int{}, false
		}
		version[i] = value
	}
	return version, true
}

func compareVersions(left, right [3]int) int {
	for i := range left {
		if left[i] < right[i] {
			return -1
		}
		if left[i] > right[i] {
			return 1
		}
	}
	return 0
}

func formatVersion(version [3]int) string {
	return fmt.Sprintf("%d.%d.%d", version[0], version[1], version[2])
}

func (r *Runner) rsyncArgs(target, destinationRoot, sourceRoot string, verify bool) []string {
	args := []string{
		"--rsh=ssh -oBatchMode=yes -oStrictHostKeyChecking=yes -oConnectTimeout=15 -oServerAliveInterval=15 -oServerAliveCountMax=2",
		"--rsync-path=env -u RSYNC_CHECKSUM_LIST rsync",
		"-s",
		"--no-iconv",
		"--no-links",
		"--no-recursive",
		"--files-from=-",
		"--from0",
		"--relative",
		"--mkpath",
		"--partial-dir=" + partialDirectory,
		"--timeout=120",
	}
	if verify {
		args = append(args, "--checksum", "--dry-run", "--itemize-changes", "--out-format=%i")
	} else {
		args = append(args, "--ignore-existing")
	}
	args = append(args, sourceRoot+string(filepath.Separator), target+":"+destinationRoot+string(filepath.Separator))
	return args
}

func (r *Runner) preflight(ctx context.Context, target, destinationRoot string, files []File) ([]bool, error) {
	sshPath, err := exec.LookPath("ssh")
	if err != nil {
		return nil, &BlockedError{Reason: fmt.Sprintf("OpenSSH client is unavailable: %v", err)}
	}
	input := preflightInput(destinationRoot, files)
	checkCtx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	output, err := runSSH(checkCtx, sshPath, target, "sh -c "+shellQuote(destinationPreflightScript), input)
	if err != nil {
		var blocked *BlockedError
		if errors.As(err, &blocked) {
			return nil, blocked
		}
		if checkCtx.Err() != nil {
			return nil, fmt.Errorf("destination preflight canceled: %w", checkCtx.Err())
		}
		if sshConfigurationError(err) || missingRemoteTool(err) {
			return nil, &BlockedError{Reason: err.Error()}
		}
		return nil, fmt.Errorf("destination preflight failed: %w", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(output), "\n"), "\n")
	if len(lines) != len(files)+1 || lines[0] != "SC1" {
		return nil, errors.New("destination preflight returned an invalid response")
	}
	exists := make([]bool, len(files))
	for i, line := range lines[1:] {
		switch line {
		case "0":
		case "1":
			exists[i] = true
		default:
			return nil, errors.New("destination preflight returned an invalid file status")
		}
	}
	return exists, nil
}

func runSSH(ctx context.Context, sshPath, target, remoteCommand string, input []byte) ([]byte, error) {
	args := []string{
		"-oBatchMode=yes",
		"-oStrictHostKeyChecking=yes",
		"-oConnectTimeout=15",
		"-oServerAliveInterval=15",
		"-oServerAliveCountMax=2",
		target,
		remoteCommand,
	}
	cmd := contextCommand(ctx, sshPath, args...)
	cmd.Env = envWithoutRsyncChecksumList()
	cmd.Stdin = bytes.NewReader(input)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		detail := strings.TrimSpace(stderr.String())
		if strings.Contains(detail, "SC_BLOCKED:") {
			return nil, &BlockedError{Reason: strings.TrimSpace(strings.SplitN(detail, "SC_BLOCKED:", 2)[1])}
		}
		if detail == "" {
			detail = strings.TrimSpace(stdout.String())
		}
		if detail != "" {
			return nil, fmt.Errorf("%w: %s", err, detail)
		}
		return nil, err
	}
	return stdout.Bytes(), nil
}

func contextCommand(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	cmd.WaitDelay = processWaitDelay
	return cmd
}

func sshConfigurationError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "host key verification failed") ||
		strings.Contains(message, "remote host identification has changed") ||
		strings.Contains(message, "bad owner or permissions") ||
		strings.Contains(message, "bad permissions") ||
		strings.Contains(message, "permission denied") ||
		strings.Contains(message, "no matching host key type") ||
		strings.Contains(message, "no matching key exchange method")
}

func missingRemoteRsync(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "rsync: not found") ||
		strings.Contains(message, "rsync: command not found") ||
		strings.Contains(message, "rsync: no such file or directory") ||
		strings.Contains(message, "'rsync': no such file or directory") ||
		strings.Contains(message, "\"rsync\": no such file or directory")
}

func unsupportedRemoteEnv(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "env: not found") ||
		strings.Contains(message, "env: command not found") ||
		strings.Contains(message, "invalid option -- 'u'") ||
		strings.Contains(message, "illegal option -- u") ||
		strings.Contains(message, "unrecognized option")
}

func missingRemoteTool(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "awk: not found") ||
		strings.Contains(message, "awk: command not found") ||
		strings.Contains(message, "sh: awk: not found")
}

func unsupportedRsyncCommand(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unknown option") ||
		strings.Contains(message, "unrecognized option") ||
		strings.Contains(message, "invalid option")
}

func runRsync(ctx context.Context, args []string, input []byte) error {
	_, err := runRsyncOutput(ctx, args, input)
	return err
}

func runRsyncOutput(ctx context.Context, args []string, input []byte) ([]byte, error) {
	cmd := contextCommand(ctx, "rsync", args...)
	cmd.Env = envWithoutRsyncChecksumList()
	cmd.Stdin = bytes.NewReader(input)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		detail := strings.TrimSpace(stderr.String())
		combined := fmt.Errorf("%w: %s", err, detail)
		if detail == "" {
			combined = err
		}
		if unsupportedRsyncCommand(combined) || missingRemoteRsync(combined) || unsupportedRemoteEnv(combined) || sshConfigurationError(combined) {
			return nil, &BlockedError{Reason: combined.Error()}
		}
		if detail != "" {
			return nil, combined
		}
		return nil, err
	}
	return stdout.Bytes(), nil
}

func fileList(files []File) []byte {
	ordered := canonicalFiles(files)
	var list bytes.Buffer
	for _, file := range ordered {
		list.WriteString(filepath.FromSlash(file.Path))
		list.WriteByte(0)
	}
	return list.Bytes()
}

func envWithoutRsyncChecksumList() []string {
	environment := os.Environ()
	filtered := make([]string, 0, len(environment))
	for _, value := range environment {
		if !strings.HasPrefix(value, "RSYNC_CHECKSUM_LIST=") {
			filtered = append(filtered, value)
		}
	}
	return filtered
}

func preflightInput(destinationRoot string, files []File) []byte {
	var input bytes.Buffer
	_, _ = fmt.Fprintf(&input, "%d\n%s\n", len(files), hex.EncodeToString([]byte(destinationRoot)))
	for _, file := range canonicalFiles(files) {
		input.WriteString(hex.EncodeToString([]byte(file.Path)))
		input.WriteByte('\n')
	}
	return input.Bytes()
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func validSSHTarget(target string) bool {
	return sshTargetPattern.MatchString(target)
}

func validateManifest(manifest Manifest) ([]File, int64, error) {
	if len(manifest.Files) == 0 {
		return nil, 0, errors.New("replication manifest has no files")
	}
	files := canonicalFiles(manifest.Files)
	seenPath := make(map[string]struct{}, len(files))
	seenIndex := make(map[int]struct{}, len(files))
	var total int64
	for _, file := range files {
		clean, err := cleanRelativePath(filepath.FromSlash(file.Path), "manifest file path")
		if err != nil || filepath.ToSlash(clean) != file.Path {
			return nil, 0, &BlockedError{Reason: fmt.Sprintf("manifest contains an unsafe file path %q", file.Path)}
		}
		if file.Size < 0 || file.Index < 0 {
			return nil, 0, &BlockedError{Reason: fmt.Sprintf("manifest contains invalid metadata for %q", file.Path)}
		}
		if _, exists := seenPath[file.Path]; exists {
			return nil, 0, &BlockedError{Reason: fmt.Sprintf("manifest has duplicate file path %q", file.Path)}
		}
		if _, exists := seenIndex[file.Index]; exists {
			return nil, 0, &BlockedError{Reason: fmt.Sprintf("manifest has duplicate file index %d", file.Index)}
		}
		seenPath[file.Path] = struct{}{}
		seenIndex[file.Index] = struct{}{}
		if file.Size > int64(^uint64(0)>>1)-total {
			return nil, 0, &BlockedError{Reason: "manifest exceeds the supported total size"}
		}
		total += file.Size
	}
	for _, file := range files {
		parent := filepath.Dir(filepath.FromSlash(file.Path))
		for parent != "." {
			if _, found := seenPath[filepath.ToSlash(parent)]; found {
				return nil, 0, &BlockedError{Reason: fmt.Sprintf("manifest file paths collide at %q", parent)}
			}
			parent = filepath.Dir(parent)
		}
	}
	if total != manifest.TotalSize {
		return nil, 0, errors.New("replication manifest total size does not match its files")
	}
	return files, total, nil
}

func validateLocalFiles(sourceRoot string, files []File) error {
	if _, err := lstatPathComponents(sourceRoot); err != nil {
		return err
	}
	for _, file := range files {
		if err := validateSourceFile(sourceRoot, filepath.FromSlash(file.Path), file.Size); err != nil {
			return err
		}
	}
	return nil
}

func itemizedFileChanges(output []byte) (int, error) {
	count := 0
	for _, line := range strings.Split(strings.TrimSuffix(string(output), "\n"), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			continue
		}
		if len(line) < 2 {
			return 0, errors.New("rsync returned invalid itemized verification output")
		}
		if line[0] == '.' {
			continue
		}
		if line[1] == 'f' && line[0] != '.' {
			count++
			continue
		}
		if line[1] == 'd' {
			continue
		}
		return 0, errors.New("rsync returned unsupported itemized verification output")
	}
	return count, nil
}

const destinationPreflightScript = `set -eu
fail() { printf '%s\n' "$1" >&2; exit 2; }
blocked() { printf 'SC_BLOCKED: %s\n' "$1" >&2; exit 2; }
command -v awk >/dev/null 2>&1 || blocked "remote awk is unavailable"
command -v env >/dev/null 2>&1 || blocked "remote env is unavailable"
decode_hex() {
	_escaped=$(printf '%s' "$1" | awk 'BEGIN { digits = "0123456789abcdef" } { if (length($0) % 2) exit 1; for (i = 1; i <= length($0); i += 2) { a = index(digits, tolower(substr($0, i, 1))) - 1; b = index(digits, tolower(substr($0, i + 1, 1))) - 1; if (a < 0 || b < 0) exit 1; printf "\\%03o", a * 16 + b } }') || fail "invalid encoded path"
	_decoded=$(printf '%b.' "$_escaped") || fail "could not decode path"
	DECODED=${_decoded%.}
}
check_directories() {
	case "$1" in /*) ;; *) return 3 ;; esac
	[ "$1" != / ] || return 3
	_rest=${1#/}
	_current=
	while [ -n "$_rest" ]; do
		case "$_rest" in */*) _part=${_rest%%/*}; _rest=${_rest#*/} ;; *) _part=$_rest; _rest= ;; esac
		[ -n "$_part" ] || return 3
		_current=${_current}/$_part
		[ ! -L "$_current" ] || return 2
		if [ -e "$_current" ]; then
			[ -d "$_current" ] || return 3
		else
			return 1
		fi
	done
	return 0
}
IFS= read -r _count || fail "missing preflight file count"
case "$_count" in ''|*[!0-9]*) fail "invalid preflight file count" ;; esac
IFS= read -r _hex_root || fail "missing destination root"
decode_hex "$_hex_root"
_root=$DECODED
case "$_root" in /*) ;; *) blocked "destination root is not absolute" ;; esac
[ "$_root" != / ] || blocked "destination root must not be the filesystem root"
if check_directories "$_root"; then
	_root_exists=1
else
	_status=$?
	case "$_status" in
		1) _root_exists=0 ;;
		2) blocked "destination root traverses a symlink" ;;
		*) blocked "destination root component is not a directory" ;;
	esac
fi
printf 'SC1\n'
_index=0
while [ "$_index" -lt "$_count" ]; do
	IFS= read -r _hex_rel || fail "missing destination file path"
	decode_hex "$_hex_rel"
	_rel=$DECODED
	case "$_rel" in
		''|/*|.|..|./*|../*|*/.|*/./*|*/..|*/../*|*//*|.seedcache-partial|.seedcache-partial/*|*/.seedcache-partial|*/.seedcache-partial/*)
			blocked "manifest contains an unsafe destination path" ;;
	esac
	if [ "$_root_exists" -eq 0 ]; then
		printf '0\n'
		_index=$((_index + 1))
		continue
	fi
	_current=$_root
	_rest=$_rel
	_missing=0
	while :; do
		case "$_rest" in */*) _part=${_rest%%/*}; _rest=${_rest#*/} ;; *) break ;; esac
		_current=${_current}/$_part
		[ ! -L "$_current" ] || blocked "destination path traverses a symlink"
		if [ -e "$_current" ]; then
			[ -d "$_current" ] || blocked "destination path component is not a directory"
		else
			_missing=1
			break
		fi
	done
	if [ "$_missing" -eq 1 ]; then
		printf '0\n'
		_index=$((_index + 1))
		continue
	fi
	_leaf=$_rest
	_target=${_current}/$_leaf
	[ ! -L "$_target" ] || blocked "destination file is a symlink"
	if [ -e "$_target" ]; then
		[ -f "$_target" ] || blocked "destination file is not a regular file"
		_exists=1
	else
		_exists=0
	fi
	_partial=${_current}/.seedcache-partial
	[ ! -L "$_partial" ] || blocked "rsync partial directory is a symlink"
	if [ -e "$_partial" ]; then
		[ -d "$_partial" ] || blocked "rsync partial path is not a directory"
		_partial_file=${_partial}/$_leaf
		[ ! -L "$_partial_file" ] || blocked "rsync partial file is a symlink"
		if [ -e "$_partial_file" ]; then
			[ -f "$_partial_file" ] || blocked "rsync partial path is not a regular file"
		fi
	fi
	printf '%s\n' "$_exists"
	_index=$((_index + 1))
done
`
