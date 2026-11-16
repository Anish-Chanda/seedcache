package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsAndParsesDuration(t *testing.T) {
	path := writeConfig(t, `qbittorrent:
  url: http://localhost:8080
  username: test-user
  password: test-pass
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.API.Listen != "0.0.0.0:8765" {
		t.Errorf("API.Listen = %q, want default", cfg.API.Listen)
	}
	if cfg.Database.Path != "/var/lib/seedcache/seedcache.db" {
		t.Errorf("Database.Path = %q, want default", cfg.Database.Path)
	}
	if cfg.Discovery.Interval != 30*time.Second {
		t.Errorf("Discovery.Interval = %s, want 30s", cfg.Discovery.Interval)
	}
	if cfg.Logging.Level != "info" {
		t.Errorf("Logging.Level = %q, want info", cfg.Logging.Level)
	}
}

func TestLoadStrictlyRejectsUnknownField(t *testing.T) {
	path := writeConfig(t, `qbittorrent:
  url: http://localhost:8080
  username: test-user
  password: test-pass
  passwrod: typo
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "passwrod") {
		t.Fatalf("Load() error = %v, want unknown field error", err)
	}
}

func TestLoadAppliesCredentialEnvironmentAndFileOverrides(t *testing.T) {
	secret := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(secret, []byte("secret with spaces\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := writeConfig(t, `qbittorrent:
  url: http://localhost:8080
  username: from-file
  password: from-file
`)
	t.Setenv("QBITTORRENT_USERNAME", "env-user")
	t.Setenv("QBITTORRENT_USERNAME_FILE", "")
	t.Setenv("QBITTORRENT_PASSWORD", "")
	t.Setenv("QBITTORRENT_PASSWORD_FILE", secret)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.QBittorrent.Username != "env-user" {
		t.Errorf("username = %q, want env override", cfg.QBittorrent.Username)
	}
	if cfg.QBittorrent.Password != "secret with spaces" {
		t.Errorf("password = %q, want file override with line ending removed", cfg.QBittorrent.Password)
	}
}

func TestLoadRejectsInvalidValuesAndAmbiguousCredentials(t *testing.T) {
	t.Run("duration", func(t *testing.T) {
		path := writeConfig(t, `qbittorrent:
  url: http://localhost:8080
  username: test-user
  password: test-pass
discovery:
  interval: soon
`)
		if _, err := Load(path); err == nil {
			t.Fatal("Load() succeeded with invalid duration")
		}
	})
	t.Run("credentials", func(t *testing.T) {
		path := writeConfig(t, `qbittorrent:
  url: http://localhost:8080
  username: config-user
  password: config-pass
`)
		t.Setenv("QBITTORRENT_USERNAME", "env-user")
		t.Setenv("QBITTORRENT_USERNAME_FILE", "secret-file")
		if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "both set") {
			t.Fatalf("Load() error = %v, want env/file ambiguity error", err)
		}
	})
	t.Run("URL", func(t *testing.T) {
		path := writeConfig(t, `qbittorrent:
  url: ftp://localhost:8080
  username: test-user
  password: test-pass
`)
		if _, err := Load(path); err == nil {
			t.Fatal("Load() succeeded with unsupported URL scheme")
		}
	})
	t.Run("required URL and credentials", func(t *testing.T) {
		for name, contents := range map[string]string{
			"URL": `qbittorrent:
  username: test-user
  password: test-pass
`,
			"username": `qbittorrent:
  url: http://localhost:8080
  password: test-pass
`,
			"password": `qbittorrent:
  url: http://localhost:8080
  username: test-user
`,
			"both credentials": `qbittorrent:
  url: http://localhost:8080
`,
		} {
			t.Run(name, func(t *testing.T) {
				path := writeConfig(t, contents)
				if _, err := Load(path); err == nil {
					t.Fatal("Load() succeeded without required URL and credentials")
				}
			})
		}
	})
	t.Run("URL credentials and query rejected", func(t *testing.T) {
		for _, address := range []string{
			"http://user:pass@localhost:8080",
			"http://localhost:8080?token=secret",
			"http://localhost:8080/#fragment",
			"http://:8080",
		} {
			t.Run(address, func(t *testing.T) {
				path := writeConfig(t, "qbittorrent:\n  url: "+address+"\n  username: test-user\n  password: test-pass\n")
				if _, err := Load(path); err == nil {
					t.Fatalf("Load() succeeded with unsafe URL %q", address)
				}
			})
		}
	})
	t.Run("secret file missing", func(t *testing.T) {
		path := writeConfig(t, `qbittorrent:
  url: http://localhost:8080
  username: test-user
  password: test-pass
`)
		t.Setenv("QBITTORRENT_PASSWORD_FILE", filepath.Join(t.TempDir(), "missing"))
		if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "QBITTORRENT_PASSWORD_FILE") {
			t.Fatalf("Load() error = %v, want secret file read error", err)
		}
	})
	t.Run("invalid API port", func(t *testing.T) {
		path := writeConfig(t, `qbittorrent:
  url: http://localhost:8080
  username: test-user
  password: test-pass
api:
  listen: 0.0.0.0:70000
`)
		if _, err := Load(path); err == nil {
			t.Fatal("Load() succeeded with invalid API port")
		}
	})
	t.Run("nonpositive interval and unknown level", func(t *testing.T) {
		for _, config := range []string{
			"discovery:\n  interval: 0s\n",
			"logging:\n  level: verbose\n",
		} {
			t.Run(strings.TrimSpace(strings.Split(config, ":")[0]), func(t *testing.T) {
				path := writeConfig(t, "qbittorrent:\n  url: http://localhost:8080\n  username: test-user\n  password: test-pass\n"+config)
				if _, err := Load(path); err == nil {
					t.Fatal("Load() succeeded with invalid config value")
				}
			})
		}
	})
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	t.Setenv("QBITTORRENT_USERNAME", "")
	t.Setenv("QBITTORRENT_PASSWORD", "")
	t.Setenv("QBITTORRENT_USERNAME_FILE", "")
	t.Setenv("QBITTORRENT_PASSWORD_FILE", "")
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
