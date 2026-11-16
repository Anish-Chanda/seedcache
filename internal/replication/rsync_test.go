package replication

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRsyncTransfersOnlyNULListedFilesAndVerifiesChecksums(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync is not installed")
	}
	sourceRoot, destinationRoot := t.TempDir(), t.TempDir()
	selectedPath := "selected dir/newline\nquote' $` [x].bin"
	selectedContent := []byte("selected payload")
	largeContent := bytes.Repeat([]byte("partial-resume-"), 1<<15)
	writePayload(t, sourceRoot, selectedPath, selectedContent)
	writePayload(t, sourceRoot, "nested/episode.mkv", []byte("episode"))
	writePayload(t, sourceRoot, "same.bin", []byte("same"))
	writePayload(t, sourceRoot, "collision.bin", []byte("new destination content"))
	writePayload(t, sourceRoot, "large.bin", largeContent)
	writePayload(t, sourceRoot, "shared/unselected.bin", []byte("must not transfer"))
	writePayload(t, destinationRoot, "same.bin", []byte("same"))
	writePayload(t, destinationRoot, "collision.bin", []byte("unrelated content"))
	partialPath := filepath.Join(destinationRoot, partialDirectory, "large.bin")
	if err := os.MkdirAll(filepath.Dir(partialPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partialPath, largeContent[:len(largeContent)/2], 0o600); err != nil {
		t.Fatal(err)
	}

	files := []File{
		{Path: selectedPath, Size: int64(len(selectedContent)), Index: 0},
		{Path: "nested/episode.mkv", Size: int64(len("episode")), Index: 1},
		{Path: "same.bin", Size: 4, Index: 2},
		{Path: "collision.bin", Size: int64(len("new destination content")), Index: 3},
		{Path: "large.bin", Size: int64(len(largeContent)), Index: 4},
	}

	args := []string{
		"--files-from=-", "--from0", "--relative", "--no-recursive", "--mkpath",
		"--partial-dir=" + partialDirectory, "--ignore-existing", "--no-whole-file",
		filepath.Clean(sourceRoot) + string(filepath.Separator), filepath.Clean(destinationRoot) + string(filepath.Separator),
	}
	if err := runLocalRsync(t, args, fileList(files)); err != nil {
		t.Fatalf("rsync transfer failed: %v", err)
	}
	for path, want := range map[string][]byte{
		selectedPath:         selectedContent,
		"nested/episode.mkv": []byte("episode"),
		"same.bin":           []byte("same"),
		"collision.bin":      []byte("unrelated content"),
		"large.bin":          largeContent,
	} {
		got, err := os.ReadFile(filepath.Join(destinationRoot, filepath.FromSlash(path)))
		if err != nil {
			t.Fatalf("read destination %q: %v", path, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("destination %q does not match expected content", path)
		}
	}
	if _, err := os.Stat(filepath.Join(destinationRoot, "shared", "unselected.bin")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unselected sibling was copied, stat error = %v", err)
	}

	verifyArgs := []string{
		"--files-from=-", "--from0", "--relative", "--no-recursive", "--checksum", "--dry-run",
		"--itemize-changes", "--out-format=%i",
		filepath.Clean(sourceRoot) + string(filepath.Separator), filepath.Clean(destinationRoot) + string(filepath.Separator),
	}
	output, err := runLocalRsyncOutput(verifyArgs, fileList(files))
	if err != nil {
		t.Fatalf("checksum dry run failed: %v", err)
	}
	changes, err := itemizedFileChanges(output)
	if err != nil {
		t.Fatalf("itemizedFileChanges() error = %v (output %q)", err, output)
	}
	if changes != 1 {
		t.Fatalf("itemizedFileChanges() = %d, want the one mismatching existing file", changes)
	}
}

func TestDestinationPreflightHandlesNewlinesAndRejectsSymlinks(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh is not installed")
	}
	root := t.TempDir()
	writePayload(t, root, "with newline\nname.bin", []byte("ok"))
	files := []File{{Path: "with newline\nname.bin", Size: 2, Index: 0}}
	output, stderr, err := runPreflightLocally(root, files)
	if err != nil {
		t.Fatalf("destination preflight failed: %v (%s)", err, stderr)
	}
	if string(output) != "SC1\n1\n" {
		t.Fatalf("preflight output = %q, want status for existing file", output)
	}

	outside := t.TempDir()
	writePayload(t, outside, "payload", []byte("outside"))
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	_, stderr, err = runPreflightLocally(root, []File{{Path: "linked/payload", Size: 7, Index: 1}})
	if err == nil || !strings.Contains(stderr, "SC_BLOCKED: destination path traverses a symlink") {
		t.Fatalf("preflight error = %v, stderr = %q; want symlink block", err, stderr)
	}
}

func TestDestinationAvailabilityChecksSizesAndRejectsSymlinks(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh is not installed")
	}
	root := t.TempDir()
	writePayload(t, root, "with newline\nname.bin", []byte("ok"))
	writePayload(t, root, "wrong-size.bin", []byte("x"))
	files := []File{
		{Path: "with newline\nname.bin", Size: 2, Index: 0},
		{Path: "wrong-size.bin", Size: 2, Index: 1},
		{Path: "missing.bin", Size: 3, Index: 2},
	}
	output, stderr, err := runAvailabilityLocally(root, files)
	if err != nil {
		t.Fatalf("destination availability check failed: %v (%s)", err, stderr)
	}
	if string(output) != "SC1\n0\n1\n0\n" {
		t.Fatalf("availability response = %q, want sorted statuses for missing, exact-size, and wrong-size files", output)
	}

	outside := t.TempDir()
	writePayload(t, outside, "payload", []byte("outside"))
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	_, stderr, err = runAvailabilityLocally(root, []File{{Path: "linked/payload", Size: 7, Index: 0}})
	if err == nil || !strings.Contains(stderr, "SC_BLOCKED: destination path traverses a symlink") {
		t.Fatalf("availability check error = %v, stderr = %q; want symlink block", err, stderr)
	}
}

func TestRestorePreflightAllowsMissingPathsAndRejectsUnsafeOrDifferentFiles(t *testing.T) {
	root := t.TempDir()
	writePayload(t, root, "same.bin", []byte("same"))
	files := []File{
		{Path: "nested/missing.bin", Size: 3, Index: 0},
		{Path: "same.bin", Size: 4, Index: 1},
	}
	present, err := preflightRestoreFiles(root, files)
	if err != nil {
		t.Fatalf("preflightRestoreFiles() error = %v", err)
	}
	if len(present) != 2 || present[0] || !present[1] {
		t.Fatalf("preflightRestoreFiles() = %v, want [false true]", present)
	}

	if err := os.Mkdir(filepath.Join(root, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "nested", partialDirectory), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(root, "nested", partialDirectory, "missing.bin")); err != nil {
		t.Fatal(err)
	}
	if _, err := preflightRestoreFiles(root, files[:1]); err == nil || !strings.Contains(err.Error(), "partial file") {
		t.Fatalf("preflightRestoreFiles() error = %v, want partial symlink rejection", err)
	}

	if err := os.Remove(filepath.Join(root, "nested", partialDirectory, "missing.bin")); err != nil {
		t.Fatal(err)
	}
	partialFile := filepath.Join(root, "nested", partialDirectory, "missing.bin")
	if err := os.WriteFile(partialFile, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	linkedFile := filepath.Join(t.TempDir(), "linked-partial")
	if err := os.Link(partialFile, linkedFile); err != nil {
		t.Fatal(err)
	}
	if _, err := preflightRestoreFiles(root, files[:1]); err == nil || !strings.Contains(err.Error(), "hard links") {
		t.Fatalf("preflightRestoreFiles() error = %v, want hard-linked partial rejection", err)
	}
	if err := os.Remove(linkedFile); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(partialFile); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "nested", partialDirectory)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "same.bin"), []byte("different size"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := preflightRestoreFiles(root, files[1:]); err == nil || !strings.Contains(err.Error(), "different size") {
		t.Fatalf("preflightRestoreFiles() error = %v, want size collision", err)
	}

	if err := os.Remove(filepath.Join(root, "same.bin")); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	if _, err := preflightRestoreFiles(root, []File{{Path: "linked/file.bin", Size: 1, Index: 0}}); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("preflightRestoreFiles() error = %v, want symlink rejection", err)
	}
}

func TestReverseRsyncCopiesSelectedFilesWithoutOverwritingExistingFiles(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync is not installed")
	}
	remoteRoot, sourceRoot := t.TempDir(), t.TempDir()
	writePayload(t, remoteRoot, "new/nested.bin", []byte("restored"))
	writePayload(t, remoteRoot, "same.bin", []byte("same"))
	writePayload(t, remoteRoot, "collision.bin", []byte("right"))
	writePayload(t, remoteRoot, "unselected.bin", []byte("not selected"))
	writePayload(t, sourceRoot, "same.bin", []byte("same"))
	writePayload(t, sourceRoot, "collision.bin", []byte("wrong"))

	files := []File{
		{Path: "new/nested.bin", Size: 8, Index: 0},
		{Path: "same.bin", Size: 4, Index: 1},
		{Path: "collision.bin", Size: 5, Index: 2},
	}
	verifyArgs := []string{
		"--files-from=-", "--from0", "--relative", "--no-recursive", "--checksum", "--dry-run",
		"--itemize-changes", "--out-format=%i",
		filepath.Clean(remoteRoot) + string(filepath.Separator), filepath.Clean(sourceRoot) + string(filepath.Separator),
	}
	for _, test := range []struct {
		file File
		want int
	}{{files[1], 0}, {files[2], 1}} {
		output, err := runLocalRsyncOutput(verifyArgs, fileList([]File{test.file}))
		if err != nil {
			t.Fatalf("checksum comparison failed: %v", err)
		}
		changes, err := itemizedFileChanges(output)
		if err != nil {
			t.Fatalf("itemizedFileChanges() error = %v (output %q)", err, output)
		}
		if changes != test.want {
			t.Errorf("itemizedFileChanges() for %q = %d, want %d", test.file.Path, changes, test.want)
		}
	}

	args := []string{
		"--files-from=-", "--from0", "--relative", "--no-recursive", "--mkpath",
		"--partial-dir=" + partialDirectory, "--ignore-existing",
		filepath.Clean(remoteRoot) + string(filepath.Separator), filepath.Clean(sourceRoot) + string(filepath.Separator),
	}
	if err := runLocalRsync(t, args, fileList(files)); err != nil {
		t.Fatalf("reverse rsync copy failed: %v", err)
	}
	for path, want := range map[string][]byte{
		"new/nested.bin": []byte("restored"),
		"same.bin":       []byte("same"),
		"collision.bin":  []byte("wrong"),
	} {
		got, err := os.ReadFile(filepath.Join(sourceRoot, filepath.FromSlash(path)))
		if err != nil {
			t.Fatalf("read restored source %q: %v", path, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("source %q = %q, want %q", path, got, want)
		}
	}
	if _, err := os.Stat(filepath.Join(sourceRoot, "unselected.bin")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unselected destination file was restored, stat error = %v", err)
	}

}

func TestRsyncCancellationPreservesPartialFileAndResumes(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync is not installed")
	}
	sourceRoot, destinationRoot := t.TempDir(), t.TempDir()
	content := bytes.Repeat([]byte("restore-interruption-"), (8<<20)/len("restore-interruption-"))
	writePayload(t, sourceRoot, "payload.bin", content)
	sourcePath := filepath.Join(sourceRoot, "payload.bin")
	destinationPath := filepath.Join(destinationRoot, "payload.bin")
	baseArgs := []string{
		"--partial-dir=" + partialDirectory,
		"--timeout=120",
		sourcePath,
		destinationRoot + string(filepath.Separator),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	slowArgs := append([]string{"--bwlimit=64k"}, baseArgs...)
	go func() {
		done <- runRsync(ctx, slowArgs, nil)
	}()
	if !waitForNonemptyTransferTemp(t, destinationRoot, "payload.bin", 5*time.Second) {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("canceled rsync did not exit")
		}
		t.Fatal("rsync did not begin writing its temporary payload")
	}
	canceledAt := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled rsync error = %v, want context cancellation", err)
		}
		if elapsed := time.Since(canceledAt); elapsed > processWaitDelay {
			t.Errorf("rsync shutdown after cancellation took %s, want at most %s", elapsed, processWaitDelay)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled rsync did not exit within the shutdown bound")
	}

	partialPath := filepath.Join(destinationRoot, partialDirectory, "payload.bin")
	partialInfo, err := os.Stat(partialPath)
	if err != nil {
		t.Fatalf("partial payload was not retained: %v", err)
	}
	if partialInfo.Size() <= 0 || partialInfo.Size() >= int64(len(content)) {
		t.Fatalf("partial payload size = %d, want between 0 and %d", partialInfo.Size(), len(content))
	}
	entries, err := os.ReadDir(destinationRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "payload.bin") {
			t.Errorf("rsync left an orphan temporary file at destination root: %q", entry.Name())
		}
	}

	if err := runRsync(context.Background(), baseArgs, nil); err != nil {
		t.Fatalf("rsync resume failed: %v", err)
	}
	got, err := os.ReadFile(destinationPath)
	if err != nil {
		t.Fatalf("read resumed payload: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("resumed payload does not match the source")
	}
}

func waitForNonemptyTransferTemp(t *testing.T, root, filename string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if !strings.Contains(entry.Name(), filename) || entry.IsDir() {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() > 0 {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestItemizedFileChangesParsesContentAndMetadataDifferences(t *testing.T) {
	changes, err := itemizedFileChanges([]byte(">f+++++++++\n.f..t......\ncd+++++++++\n"))
	if err != nil {
		t.Fatalf("itemizedFileChanges() error = %v", err)
	}
	if changes != 1 {
		t.Fatalf("itemizedFileChanges() = %d, want only content difference", changes)
	}
	if _, err := itemizedFileChanges([]byte("unexpected output\n")); err == nil {
		t.Fatal("itemizedFileChanges() accepted an unsupported item line")
	}
}

func TestRsyncVersionAndTargetValidation(t *testing.T) {
	for _, test := range []struct {
		output string
		want   [3]int
		ok     bool
	}{
		{output: "rsync  version 3.4.1  protocol version 32", want: [3]int{3, 4, 1}, ok: true},
		{output: "rsync  version 3.2.2  protocol version 31", want: [3]int{3, 2, 2}, ok: true},
		{output: "no version", ok: false},
	} {
		got, ok := parseRsyncVersion([]byte(test.output))
		if got != test.want || ok != test.ok {
			t.Errorf("parseRsyncVersion(%q) = %v, %t; want %v, %t", test.output, got, ok, test.want, test.ok)
		}
	}
	if compareVersions([3]int{3, 2, 2}, [3]int{3, 2, 3}) >= 0 || compareVersions([3]int{3, 3, 0}, [3]int{3, 2, 3}) <= 0 {
		t.Fatal("compareVersions() returned the wrong ordering")
	}
	for target, want := range map[string]bool{
		"phase2-test":        true,
		"user@host.example":  true,
		"-oProxyCommand=bad": false,
		"host;touch":         false,
		"host path":          false,
	} {
		if got := validSSHTarget(target); got != want {
			t.Errorf("validSSHTarget(%q) = %t, want %t", target, got, want)
		}
	}
}

func runLocalRsync(t *testing.T, args []string, input []byte) error {
	t.Helper()
	_, err := runLocalRsyncOutput(args, input)
	return err
}

func runLocalRsyncOutput(args []string, input []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := contextCommand(ctx, "rsync", args...)
	cmd.Env = envWithoutRsyncChecksumList()
	cmd.Stdin = bytes.NewReader(input)
	return cmd.CombinedOutput()
}

func runPreflightLocally(root string, files []File) ([]byte, string, error) {
	cmd := exec.Command("sh", "-c", destinationPreflightScript)
	cmd.Stdin = bytes.NewReader(preflightInput(root, files))
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.Bytes(), stderr.String(), err
}

func runAvailabilityLocally(root string, files []File) ([]byte, string, error) {
	cmd := exec.Command("sh", "-c", destinationAvailabilityScript)
	cmd.Stdin = bytes.NewReader(availabilityInput(root, files))
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.Bytes(), stderr.String(), err
}

func TestSSHConfigurationErrorsRequireIntervention(t *testing.T) {
	for _, tt := range []struct {
		message string
		blocked bool
	}{
		{"Bad owner or permissions on /var/lib/seedcache/.ssh/config", true},
		{"Load key: bad permissions", true},
		{"Permission denied (publickey)", true},
		{"Host key verification failed", true},
		{"Connection refused", false},
		{"Connection timed out", false},
	} {
		if got := sshConfigurationError(errors.New(tt.message)); got != tt.blocked {
			t.Errorf("sshConfigurationError(%q) = %t, want %t", tt.message, got, tt.blocked)
		}
	}
}
