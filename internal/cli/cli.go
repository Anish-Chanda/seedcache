package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/anish-chanda/seedcache/internal/api"
	"github.com/anish-chanda/seedcache/internal/config"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

const requestTimeout = 15 * time.Second

type options struct {
	configPath string
	server     string
	jsonOutput bool
}

// New builds the seedcache command tree without depending on the daemon app.
func New(version string, runDaemon func(context.Context, string) error) *cobra.Command {
	opts := &options{configPath: "config.yaml", server: defaultServerURL()}

	root := &cobra.Command{
		Use:           "seedcache",
		Short:         "Inspect and manage the seedcache daemon",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&opts.configPath, "config", opts.configPath, "path to the daemon configuration file")
	root.PersistentFlags().StringVar(&opts.server, "server", opts.server, "seedcache API base URL")
	root.PersistentFlags().BoolVar(&opts.jsonOutput, "json", false, "write machine-readable JSON")

	root.AddCommand(newDaemonCommand(opts, runDaemon))
	root.AddCommand(newStatusCommand(opts))
	root.AddCommand(newTorrentsCommand(opts))
	root.AddCommand(newVersionCommand(version, opts))
	root.AddCommand(newConfigCommand(opts))
	return root
}

func defaultServerURL() string {
	if value := strings.TrimSpace(os.Getenv("SEEDCACHE_API_URL")); value != "" {
		return value
	}
	return "http://localhost:8765"
}

func newDaemonCommand(opts *options, run func(context.Context, string) error) *cobra.Command {
	return &cobra.Command{
		Use:   "daemon",
		Short: "Run the seedcache daemon",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if run == nil {
				return errors.New("daemon runner is unavailable")
			}
			return run(cmd.Context(), opts.configPath)
		},
	}
}

func newStatusCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show daemon and qBittorrent status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var status api.Status
			if err := newAPIClient(opts.server).get(cmd.Context(), "/api/v1/status", &status); err != nil {
				return err
			}
			if opts.jsonOutput {
				return writeJSON(cmd.OutOrStdout(), status)
			}
			return writeStatus(cmd.OutOrStdout(), status)
		},
	}
}

func newTorrentsCommand(opts *options) *cobra.Command {
	command := &cobra.Command{
		Use:   "torrents",
		Short: "List torrents",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var result api.TorrentList
			if err := newAPIClient(opts.server).get(cmd.Context(), "/api/v1/torrents", &result); err != nil {
				return err
			}
			if opts.jsonOutput {
				return writeJSON(cmd.OutOrStdout(), result)
			}
			return writeTorrents(cmd.OutOrStdout(), result.Torrents)
		},
	}
	command.AddCommand(newTorrentShowCommand(opts))
	return command
}

func newTorrentShowCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "show <hash>",
		Short: "Show torrent details and its file manifest",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			hash := strings.TrimSpace(args[0])
			if hash == "" {
				return errors.New("torrent hash must not be empty")
			}
			var detail api.TorrentDetail
			path := "/api/v1/torrents/" + url.PathEscape(hash)
			if err := newAPIClient(opts.server).get(cmd.Context(), path, &detail); err != nil {
				return err
			}
			if opts.jsonOutput {
				return writeJSON(cmd.OutOrStdout(), detail)
			}
			return writeTorrentDetail(cmd.OutOrStdout(), detail)
		},
	}
}

func newVersionCommand(version string, opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show the seedcache CLI version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			result := api.Version{Version: version}
			if opts.jsonOutput {
				return writeJSON(cmd.OutOrStdout(), result)
			}
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "seedcache %s\n", version)
			return err
		},
	}
}

func newConfigCommand(opts *options) *cobra.Command {
	command := &cobra.Command{
		Use:   "config",
		Short: "Inspect configuration",
	}
	command.AddCommand(&cobra.Command{
		Use:   "validate",
		Short: "Validate the configuration file",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if _, err := config.Load(opts.configPath); err != nil {
				return fmt.Errorf("validate configuration: %w", err)
			}
			if opts.jsonOutput {
				return writeJSON(cmd.OutOrStdout(), struct {
					Valid bool `json:"valid"`
				}{Valid: true})
			}
			_, err := fmt.Fprintln(cmd.OutOrStdout(), "Configuration is valid.")
			return err
		},
	})
	return command
}

type apiClient struct {
	baseURL string
	http    *http.Client
}

func newAPIClient(baseURL string) *apiClient {
	return &apiClient{
		baseURL: baseURL,
		http:    &http.Client{Timeout: requestTimeout},
	}
}

func (c *apiClient) get(ctx context.Context, endpoint string, target any) error {
	requestURL, err := c.endpoint(endpoint)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return fmt.Errorf("create API request: %w", err)
	}
	response, err := c.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("API request canceled: %w", ctx.Err())
		}
		return fmt.Errorf("request seedcache API: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		var envelope api.ErrorResponse
		if err := json.NewDecoder(response.Body).Decode(&envelope); err == nil && (envelope.Error.Code != "" || envelope.Error.Message != "") {
			if envelope.Error.Code != "" {
				return fmt.Errorf("API error %s: %s", envelope.Error.Code, envelope.Error.Message)
			}
			return fmt.Errorf("API error: %s", envelope.Error.Message)
		}
		return fmt.Errorf("API request failed: %s", response.Status)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("seedcache API returned an empty response")
		}
		return fmt.Errorf("decode seedcache API response: %w", err)
	}
	return nil
}

func (c *apiClient) endpoint(path string) (string, error) {
	base, err := url.Parse(strings.TrimSpace(c.baseURL))
	if err != nil || base == nil {
		return "", fmt.Errorf("invalid API server URL %q", c.baseURL)
	}
	if (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return "", fmt.Errorf("invalid API server URL %q: expected an http(s) URL without credentials, query, or fragment", c.baseURL)
	}
	decodedPath, err := url.PathUnescape(path)
	if err != nil {
		return "", fmt.Errorf("invalid API path %q: %w", path, err)
	}
	basePath := strings.TrimRight(base.Path, "/")
	escapedBasePath := strings.TrimRight(base.EscapedPath(), "/")
	base.Path = basePath + decodedPath
	base.RawPath = escapedBasePath + path
	return base.String(), nil
}

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func writeStatus(writer io.Writer, status api.Status) error {
	if _, err := fmt.Fprintf(writer, "Started: %s\n", status.StartedAt.Format(time.RFC3339)); err != nil {
		return err
	}
	if status.QBittorrent.Connected {
		connection := "connected"
		if status.QBittorrent.Version != "" {
			connection += " (" + cleanCell(status.QBittorrent.Version) + ")"
		}
		if status.QBittorrent.APIVersion != "" {
			connection += " API " + cleanCell(status.QBittorrent.APIVersion)
		}
		if _, err := fmt.Fprintf(writer, "qBittorrent: %s\n", connection); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintln(writer, "qBittorrent: disconnected"); err != nil {
			return err
		}
		if status.QBittorrent.Error != "" {
			if _, err := fmt.Fprintf(writer, "Connection error: %s\n", cleanCell(status.QBittorrent.Error)); err != nil {
				return err
			}
		}
	}
	if status.QBittorrent.LastSuccess != nil {
		if _, err := fmt.Fprintf(writer, "Last successful connection: %s\n", status.QBittorrent.LastSuccess.Format(time.RFC3339)); err != nil {
			return err
		}
	}
	if status.QBittorrent.AutoTMMEnabled != nil {
		if _, err := fmt.Fprintf(writer, "Auto torrent management: %t\n", *status.QBittorrent.AutoTMMEnabled); err != nil {
			return err
		}
		if *status.QBittorrent.AutoTMMEnabled {
			if _, err := fmt.Fprintln(writer, "Disable auto torrent management for SeedCache-managed torrents."); err != nil {
				return err
			}
		}
	}
	_, err := fmt.Fprintf(writer, "Torrents: %d\n", status.TorrentCount)
	return err
}

func writeTorrents(writer io.Writer, torrents []api.Torrent) error {
	if len(torrents) == 0 {
		_, err := fmt.Fprintln(writer, "No torrents.")
		return err
	}
	w := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "NAME\tSIZE\tPROGRESS\tRATIO\tSTATE\tHASH"); err != nil {
		return err
	}
	for _, torrent := range torrents {
		if _, err := fmt.Fprintf(w, "%s\t%s\t%.1f%%\t%.2f\t%s\t%s\n",
			cleanCell(torrent.Name), humanize.Bytes(uint64(max(torrent.TotalSize, int64(0)))), torrent.Progress*100,
			torrent.Ratio, cleanCell(torrent.State), cleanCell(torrent.Hash)); err != nil {
			return err
		}
	}
	return w.Flush()
}

func writeTorrentDetail(writer io.Writer, detail api.TorrentDetail) error {
	t := detail.Torrent
	if _, err := fmt.Fprintf(writer, "Name: %s\nHash: %s\nState: %s\nProgress: %.1f%%\nSize: %s\nUploaded: %s\nRatio: %.2f\nSeeding time: %s\nSave path: %s\nAuto torrent management: %t\n",
		cleanCell(t.Name), cleanCell(t.Hash), cleanCell(t.State), t.Progress*100,
		humanize.Bytes(uint64(max(t.TotalSize, int64(0)))), humanize.Bytes(uint64(max(t.Uploaded, int64(0)))), t.Ratio,
		time.Duration(max(t.SeedingSeconds, int64(0)))*time.Second, cleanCell(t.SavePath), t.AutoTMM); err != nil {
		return err
	}
	if t.CompletedAt != nil {
		if _, err := fmt.Fprintf(writer, "Completed at: %s\n", t.CompletedAt.Format(time.RFC3339)); err != nil {
			return err
		}
	}
	if t.AutoTMM {
		if _, err := fmt.Fprintln(writer, "Warning: auto torrent management is enabled for this torrent; disable it for SeedCache-managed torrents."); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(writer, "\nFiles:"); err != nil {
		return err
	}
	if len(detail.Files) == 0 {
		_, err := fmt.Fprintln(writer, "No files.")
		return err
	}
	w := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "INDEX\tPATH\tSIZE\tPRIORITY\tPROGRESS"); err != nil {
		return err
	}
	for _, file := range detail.Files {
		if _, err := fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%.1f%%\n", file.Index, cleanCell(file.Path),
			humanize.Bytes(uint64(max(file.Size, int64(0)))), file.Priority, file.Progress*100); err != nil {
			return err
		}
	}
	return w.Flush()
}

func cleanCell(value string) string {
	return strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(value)
}
