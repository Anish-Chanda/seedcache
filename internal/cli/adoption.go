package cli

import (
	"fmt"
	"io"
	"net/url"
	"text/tabwriter"

	"github.com/anish-chanda/seedcache/internal/replication"
	"github.com/spf13/cobra"
)

func newAdoptCommand(opts *options) *cobra.Command {
	var all, dryRun, apply bool
	cmd := &cobra.Command{
		Use:   "adopt <hash> | --all (--dry-run | --apply)",
		Short: "Register existing remote payloads without copying or verifying their contents",
		Args: func(cmd *cobra.Command, args []string) error {
			if dryRun && apply {
				return fmt.Errorf("--dry-run and --apply cannot be combined")
			}
			if all {
				if len(args) != 0 {
					return fmt.Errorf("--all cannot be combined with a torrent hash")
				}
				if !dryRun && !apply {
					return fmt.Errorf("bulk adoption requires --dry-run or --apply")
				}
			} else {
				if len(args) != 1 {
					return fmt.Errorf("specify one torrent hash or --all")
				}
				if apply {
					return fmt.Errorf("--apply is only used with --all")
				}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			client := newAPIClient(opts.server)
			client.http.Timeout = 0
			query := url.Values{}
			if dryRun {
				query.Set("dry_run", "true")
			} else if all {
				query.Set("apply", "true")
			}
			endpoint := "/api/v1/adoptions"
			if !all {
				hash, err := transferHash(args[0])
				if err != nil {
					return err
				}
				endpoint = "/api/v1/torrents/" + url.PathEscape(hash) + "/adopt"
			}
			if len(query) > 0 {
				endpoint += "?" + query.Encode()
			}
			if all {
				var report replication.AdoptionReport
				if err := client.post(cmd.Context(), endpoint, &report); err != nil {
					return err
				}
				if opts.jsonOutput {
					return writeJSON(cmd.OutOrStdout(), report)
				}
				return writeAdoptionReport(cmd.OutOrStdout(), report)
			}
			var result replication.AdoptionResult
			if err := client.post(cmd.Context(), endpoint, &result); err != nil {
				return err
			}
			if opts.jsonOutput {
				return writeJSON(cmd.OutOrStdout(), result)
			}
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "Torrent %s: %s\n", cleanCell(result.Hash), cleanCell(result.Status))
			return err
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "adopt torrents in the current qBittorrent catalog")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "check remote files without changing records or payloads")
	cmd.Flags().BoolVar(&apply, "apply", false, "apply bulk adoption")
	return cmd
}

func writeAdoptionReport(writer io.Writer, report replication.AdoptionReport) error {
	w := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "HASH\tRESULT\tERROR"); err != nil {
		return err
	}
	for _, result := range report.Results {
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\n", cleanCell(result.Hash), cleanCell(result.Status), cleanCell(result.Error)); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	s := report.Summary
	_, err := fmt.Fprintf(writer, "\nAdopted: %d\nWould adopt: %d\nAlready adopted: %d\nAlready verified: %d\nMissing remote: %d\nBlocked: %d\nErrors: %d\n", s.Adopted, s.WouldAdopt, s.AlreadyAdopted, s.AlreadyVerified, s.MissingRemote, s.Blocked, s.Errors)
	return err
}
