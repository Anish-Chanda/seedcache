package config

import (
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	QBittorrent QBittorrentConfig `yaml:"qbittorrent"`
	API         APIConfig         `yaml:"api"`
	Database    DatabaseConfig    `yaml:"database"`
	Discovery   DiscoveryConfig   `yaml:"discovery"`
	Logging     LoggingConfig     `yaml:"logging"`
	Source      SourceConfig      `yaml:"source"`
	Destination DestinationConfig `yaml:"destination"`
}

type SourceConfig struct {
	QBittorrentBasePath string `yaml:"qbittorrent_base_path"`
	LocalBasePath       string `yaml:"local_base_path"`
}

type DestinationConfig struct {
	SSHTarget string `yaml:"ssh_target"`
	BasePath  string `yaml:"base_path"`
}

func (c Config) ReplicationEnabled() bool {
	return c.Source.QBittorrentBasePath != "" && c.Source.LocalBasePath != "" && c.Destination.SSHTarget != "" && c.Destination.BasePath != ""
}

type QBittorrentConfig struct {
	URL      string `yaml:"url"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type APIConfig struct {
	Listen string `yaml:"listen"`
}

type DatabaseConfig struct {
	Path string `yaml:"path"`
}

type DiscoveryConfig struct {
	Interval time.Duration `yaml:"interval"`
}

type LoggingConfig struct {
	Level string `yaml:"level"`
}

func Load(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		return Config{}, fmt.Errorf("config path is required")
	}

	cfg := Config{
		API:       APIConfig{Listen: "0.0.0.0:8765"},
		Database:  DatabaseConfig{Path: "/var/lib/seedcache/seedcache.db"},
		Discovery: DiscoveryConfig{Interval: 30 * time.Second},
		Logging:   LoggingConfig{Level: "info"},
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config %q: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Config{}, fmt.Errorf("decode config %q: multiple YAML documents are not supported", path)
		}
		return Config{}, fmt.Errorf("decode config %q: %w", path, err)
	}

	cfg.QBittorrent.URL = strings.TrimSpace(cfg.QBittorrent.URL)
	cfg.API.Listen = strings.TrimSpace(cfg.API.Listen)
	cfg.Database.Path = strings.TrimSpace(cfg.Database.Path)
	cfg.Logging.Level = strings.ToLower(strings.TrimSpace(cfg.Logging.Level))
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}

	if cfg.QBittorrent.Username, err = credential("QBITTORRENT_USERNAME", cfg.QBittorrent.Username); err != nil {
		return Config{}, err
	}
	if cfg.QBittorrent.Password, err = credential("QBITTORRENT_PASSWORD", cfg.QBittorrent.Password); err != nil {
		return Config{}, err
	}

	if err := validate(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func credential(name, configured string) (string, error) {
	value, valueSet := os.LookupEnv(name)
	file, fileSet := os.LookupEnv(name + "_FILE")
	valueSet = valueSet && value != ""
	fileSet = fileSet && file != ""
	if valueSet && fileSet {
		return "", fmt.Errorf("%s and %s_FILE are both set; configure only one", name, name)
	}
	if valueSet {
		return value, nil
	}
	if !fileSet {
		return configured, nil
	}

	data, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("read %s_FILE %q: %w", name, file, err)
	}
	// Secret files commonly have a final newline. Remove line endings only so
	// passwords with intentional leading or trailing spaces remain intact.
	return strings.TrimRight(string(data), "\r\n"), nil
}

func validate(cfg Config) error {
	if cfg.QBittorrent.URL == "" {
		return fmt.Errorf("qbittorrent.url is required")
	}
	parsed, err := url.Parse(cfg.QBittorrent.URL)
	if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawFragment != "" || strings.Contains(cfg.QBittorrent.URL, "#") {
		return fmt.Errorf("qbittorrent.url must be an http or https URL without credentials, query, or fragment")
	}
	if (cfg.QBittorrent.Username == "") != (cfg.QBittorrent.Password == "") {
		return fmt.Errorf("qbittorrent.username and qbittorrent.password must be configured together")
	}
	if cfg.QBittorrent.Username == "" {
		return fmt.Errorf("qbittorrent.username and qbittorrent.password are required")
	}
	if cfg.API.Listen == "" {
		return fmt.Errorf("api.listen is required")
	}
	_, portText, err := net.SplitHostPort(cfg.API.Listen)
	if err != nil {
		return fmt.Errorf("api.listen must be a host:port address: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("api.listen port must be between 1 and 65535")
	}
	if cfg.Database.Path == "" {
		return fmt.Errorf("database.path is required")
	}
	if cfg.Discovery.Interval <= 0 {
		return fmt.Errorf("discovery.interval must be greater than zero")
	}
	if err := validateReplication(cfg); err != nil {
		return err
	}
	switch cfg.Logging.Level {
	case "trace", "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("logging.level must be one of trace, debug, info, warn, or error")
	}
	return nil
}

func validateReplication(cfg Config) error {
	if cfg.Source == (SourceConfig{}) && cfg.Destination == (DestinationConfig{}) {
		return nil
	}
	if !cfg.ReplicationEnabled() {
		return fmt.Errorf("replication requires source.qbittorrent_base_path, source.local_base_path, destination.ssh_target, and destination.base_path")
	}
	for key, value := range map[string]string{
		"source.qbittorrent_base_path": cfg.Source.QBittorrentBasePath,
		"destination.base_path":        cfg.Destination.BasePath,
	} {
		if !path.IsAbs(value) || path.Clean(value) != value || value == "/" || strings.ContainsRune(value, 0) {
			return fmt.Errorf("%s must be a clean absolute directory path other than /", key)
		}
	}
	local := cfg.Source.LocalBasePath
	if !filepath.IsAbs(local) || filepath.Clean(local) != local || local == string(filepath.Separator) || strings.ContainsRune(local, 0) {
		return fmt.Errorf("source.local_base_path must be a clean absolute directory path other than /")
	}
	if !regexp.MustCompile(`^([a-zA-Z0-9_][a-zA-Z0-9_.-]*@)?[a-zA-Z0-9_][a-zA-Z0-9_.-]*$`).MatchString(cfg.Destination.SSHTarget) {
		return fmt.Errorf("destination.ssh_target must be an SSH host alias or [user@]hostname; configure ports and other options in SSH config")
	}
	return nil
}
