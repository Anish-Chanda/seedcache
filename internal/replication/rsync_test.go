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
