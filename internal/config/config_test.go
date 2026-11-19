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

func TestReplicationTimeoutConfiguration(t *testing.T) {
	base := "qbittorrent:\n  url: http://localhost:8080\n  username: user\n  password: pass\n"
	cfg, err := Load(writeConfig(t, base))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Replication.TransferTimeout != 2*time.Minute || cfg.Replication.VerificationTimeout != 30*time.Minute {
		t.Fatalf("unexpected replication timeout defaults: %+v", cfg.Replication)
	}
	cfg, err = Load(writeConfig(t, base+"replication:\n  transfer_timeout: 5m\n  verification_timeout: 45m\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Replication.TransferTimeout != 5*time.Minute || cfg.Replication.VerificationTimeout != 45*time.Minute {
		t.Fatalf("configured replication timeouts not applied: %+v", cfg.Replication)
	}
	for _, bad := range []string{
		"  transfer_timeout: 0s\n",
		"  transfer_timeout: -1s\n",
		"  verification_timeout: 0s\n",
		"  verification_timeout: 1ms\n",
		"  verification_timeout: 1.5s\n",
	} {
		if _, err := Load(writeConfig(t, base+"replication:\n"+bad)); err == nil {
			t.Errorf("accepted invalid timeout %q", bad)
		}
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

func TestReplicationConfiguration(t *testing.T) {
	base := "qbittorrent:\n  url: http://localhost:8080\n  username: user\n  password: pass\n"
	valid := "source:\n  qbittorrent_base_path: /downloads\n  local_base_path: /data/torrents\ndestination:\n  ssh_target: storage\n  base_path: /mnt/storage/torrents\n"
	for _, tt := range []struct {
		name      string
		extra     string
		wantError bool
		enabled   bool
	}{
		{"discovery only", "", false, false},
		{"complete", valid, false, true},
		{"user alias", strings.Replace(valid, "ssh_target: storage", "ssh_target: user@storage", 1), false, true},
		{"partial", "source:\n  local_base_path: /data/torrents\n", true, false},
		{"relative source", strings.Replace(valid, "/downloads", "downloads", 1), true, false},
		{"traversal", strings.Replace(valid, "/downloads", "/downloads/../etc", 1), true, false},
		{"root", strings.Replace(valid, "/mnt/storage/torrents", "/", 1), true, false},
		{"ssh options", strings.Replace(valid, "ssh_target: storage", "ssh_target: '-oProxyCommand=bad'", 1), true, false},
		{"remote path syntax", strings.Replace(valid, "ssh_target: storage", "ssh_target: 'host:/tmp'", 1), true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, base+tt.extra))
			if (err != nil) != tt.wantError {
				t.Fatalf("Load error = %v, want error %v", err, tt.wantError)
			}
			if err == nil && cfg.ReplicationEnabled() != tt.enabled {
				t.Fatalf("enabled = %v", cfg.ReplicationEnabled())
			}
		})
	}
}

func TestRetentionConfiguration(t *testing.T) {
	base := "qbittorrent:\n  url: http://localhost:8080\n  username: user\n  password: pass\n"
	cfg, err := Load(writeConfig(t, base))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Retention.AutoEvict || cfg.Retention.MinRatio != 1 || cfg.Retention.MinSeedingTime != 72*time.Hour || cfg.Retention.LowFreePercent != 10 || cfg.Retention.TargetFreePercent != 20 {
		t.Fatalf("unexpected retention defaults: %+v", cfg.Retention)
	}
	for _, extra := range []string{
		"  min_ratio: -1\n", "  min_ratio: .nan\n", "  min_ratio: .inf\n",
		"  min_seeding_time: -1h\n", "  low_free_percent: -1\n", "  low_free_percent: 20\n",
		"  target_free_percent: 10\n", "  target_free_percent: 101\n", "  target_free_percent: .nan\n",
		"  auto_evict: true\n",
	} {
		t.Run(strings.TrimSpace(extra), func(t *testing.T) {
			if _, err := Load(writeConfig(t, base+"retention:\n"+extra)); err == nil {
				t.Fatal("accepted invalid retention configuration")
			}
		})
	}
	cfg, err = Load(writeConfig(t, base+"source:\n  qbittorrent_base_path: /downloads\n  local_base_path: /data/torrents\ndestination:\n  ssh_target: storage\n  base_path: /torrents\nretention:\n  auto_evict: true\n  min_ratio: 0\n  min_seeding_time: 0s\n  low_free_percent: 0\n  target_free_percent: 100\n"))
	if err != nil || !cfg.Retention.AutoEvict {
		t.Fatalf("valid configured retention: %+v, %v", cfg.Retention, err)
	}
}
