package cmd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"tm1cli/internal/client"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

var (
	sessionsUser        string
	sessionsLimit       int
	sessionsAll         bool
	sessionsCloseYes    bool
	sessionsCloseDryRun bool
)

const (
	sessionsListBase     = "Sessions?$select=ID,Context,Active&$expand=User($select=Name)"
	sessionsThreadsParam = ",Threads($select=ID)"
	// Over-fetch buffer above --limit so the client can detect that the
	// server actually had more rows and emit "Showing 50 of N" instead of
	// silently capping at exactly --limit.
	sessionsTruncationProbe = 100
)

var sessionsCmd = &cobra.Command{
	Use:   "sessions",
	Short: "Manage active TM1 sessions",
	Long:  `Manage and inspect active sessions on the TM1 server.`,
}

var sessionsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List active sessions",
	Long: `List active sessions on the TM1 server.

REST API: GET /Sessions

Results are limited to 50 by default. Use --all to show all sessions.`,
	Example: `  tm1cli sessions list
  tm1cli sessions list --user admin
  tm1cli sessions list --all
  tm1cli sessions list --output json`,
	RunE: runSessionsList,
}

var sessionsCloseCmd = &cobra.Command{
	Use:   "close <id>",
	Short: "Close an active session",
	Long: `Close an active session on the TM1 server.

REST API: POST /Sessions(<id>)/tm1.Close

The command prompts for confirmation only when closing your OWN active
session (the one this CLI is authenticated through), since closing it
will log you out. Use --yes to skip that prompt for scripting.

Use --dry-run to preview the action without actually closing the session.`,
	Example: `  tm1cli sessions close 123
  tm1cli sessions close 123 --yes
  tm1cli sessions close 123 --dry-run
  tm1cli sessions close 123 --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runSessionsClose,
}

func runSessionsList(cmd *cobra.Command, args []string) error {
	cfg, err := loadConfig()
	if err != nil {
		output.PrintError(err.Error(), isJSONOutput(nil))
		return errSilent
	}

	jsonMode := isJSONOutput(cfg)
	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	limit := getLimit(cfg, sessionsLimit, sessionsAll)
	activeFilters := sessionsUser != ""

	sessions, threadsAvailable, err := fetchSessions(cl, limit, activeFilters)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	filtered := filterSessions(sessions, sessionsUser)
	displaySessions(filtered, len(filtered), limit, jsonMode, threadsAvailable)
	return nil
}

// fetchSessions issues GET /Sessions with the full $expand. If the server
// rejects $expand=Threads (HTTP 400/501 with an expand-related body), it
// retries without that clause and returns threadsAvailable=false. The retry
// path returns retryErr on retry failure (so the user sees the actual second
// failure), and the original error otherwise.
func fetchSessions(cl *client.Client, limit int, activeFilters bool) ([]model.Session, bool, error) {
	topClause := ""
	if limit > 0 && !activeFilters {
		topClause = fmt.Sprintf("&$top=%d", limit+sessionsTruncationProbe)
	}

	data, err := cl.Get(sessionsListBase + sessionsThreadsParam + topClause)
	if err != nil {
		if !isExpandRejection(err) {
			return nil, false, err
		}
		retryData, retryErr := cl.Get(sessionsListBase + topClause)
		if retryErr != nil {
			return nil, false, retryErr
		}
		output.PrintWarning("server rejected $expand=Threads — Threads column unavailable")
		var resp model.SessionResponse
		if jsonErr := json.Unmarshal(retryData, &resp); jsonErr != nil {
			return nil, false, fmt.Errorf("Cannot parse server response.")
		}
		return resp.Value, false, nil
	}

	var resp model.SessionResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, false, fmt.Errorf("Cannot parse server response.")
	}
	return resp.Value, true, nil
}

// isExpandRejection reports whether err is a 400/501 OData rejection that
// looks like an $expand-related complaint. Auth errors (401/403/auth-fail)
// and not-found are excluded; the keyword guard avoids retrying on
// unrelated 400s such as bad $select syntax.
func isExpandRejection(err error) bool {
	if err == nil || errors.Is(err, client.ErrNotFound) {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "Authentication failed") || strings.HasPrefix(msg, "HTTP 403") {
		return false
	}
	if !strings.HasPrefix(msg, "HTTP 400") && !strings.HasPrefix(msg, "HTTP 501") {
		return false
	}
	lower := strings.ToLower(msg)
	return strings.Contains(lower, "expand") ||
		strings.Contains(lower, "thread") ||
		strings.Contains(lower, "navigation") ||
		strings.Contains(lower, "not supported") ||
		strings.Contains(lower, "not implemented")
}

func filterSessions(sessions []model.Session, user string) []model.Session {
	if user == "" {
		return sessions
	}
	userLower := strings.ToLower(user)
	var out []model.Session
	for _, s := range sessions {
		if !strings.Contains(strings.ToLower(s.User.Name), userLower) {
			continue
		}
		out = append(out, s)
	}
	return out
}

func displaySessions(sessions []model.Session, total int, limit int, jsonMode bool, threadsAvailable bool) {
	shown := sessions
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}

	if jsonMode {
		output.PrintJSON(shown)
		return
	}

	headers := []string{"ID", "USER", "CONTEXT", "ACTIVE", "THREADS"}
	rows := make([][]string, len(shown))
	for i, s := range shown {
		threadsCell := "-"
		if threadsAvailable {
			threadsCell = strconv.Itoa(len(s.Threads))
		}
		rows[i] = []string{
			strconv.FormatInt(s.ID, 10),
			s.User.Name,
			s.Context,
			strconv.FormatBool(s.Active),
			threadsCell,
		}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(shown), total, "--user to filter or --all")
}

func runSessionsClose(cmd *cobra.Command, args []string) error {
	id := args[0]

	cfg, err := loadConfig()
	if err != nil {
		output.PrintError(err.Error(), isJSONOutput(nil))
		return errSilent
	}

	jsonMode := isJSONOutput(cfg)

	idNum, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		output.PrintError(fmt.Sprintf("Invalid session ID %q (must be numeric).", id), jsonMode)
		return errSilent
	}

	if sessionsCloseDryRun {
		if jsonMode {
			output.PrintJSON(map[string]string{"status": "dry-run", "id": id})
		} else {
			fmt.Printf("[dry-run] Would close session '%s'.\n", id)
		}
		return nil
	}

	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	if !sessionsCloseYes {
		isSelfClose, selfErr := detectSelfClose(cl, idNum)
		if selfErr != nil {
			output.PrintWarning("could not verify whether this is your active session — proceeding")
			if flagVerbose {
				fmt.Fprintf(os.Stderr, "[verbose] ActiveSession lookup error: %s\n", selfErr)
			}
		}
		if isSelfClose {
			fmt.Fprintf(os.Stderr, "Warning: '%s' is your active session. Closing it will log you out.\n", id)
			if !promptYesNo(bufio.NewReader(os.Stdin), "Continue?") {
				return nil
			}
		}
	}

	// Unquoted numeric key per OData URL conventions; id was validated as
	// digits-only above, so it is safe to interpolate without escaping.
	endpoint := fmt.Sprintf("Sessions(%s)/tm1.Close", id)
	if _, err := cl.Post(endpoint, map[string]interface{}{}); err != nil {
		if errors.Is(err, client.ErrNotFound) {
			output.PrintError(fmt.Sprintf("Session '%s' not found.", id), jsonMode)
			return errSilent
		}
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	if jsonMode {
		output.PrintJSON(map[string]string{"status": "closed", "id": id})
	} else {
		fmt.Printf("Closed session '%s'.\n", id)
	}
	return nil
}

// detectSelfClose looks up the current authenticated session via
// /ActiveSession and reports whether its ID equals target. Comparison is
// numeric so leading-zero variants (e.g. "00123" vs "123") match.
func detectSelfClose(cl *client.Client, target uint64) (bool, error) {
	data, err := cl.Get("ActiveSession?$select=ID")
	if err != nil {
		return false, err
	}
	var ref model.ActiveSessionRef
	if err := json.Unmarshal(data, &ref); err != nil {
		return false, fmt.Errorf("cannot parse ActiveSession response")
	}
	return ref.ID >= 0 && uint64(ref.ID) == target, nil
}

func init() {
	rootCmd.AddCommand(sessionsCmd)
	sessionsCmd.AddCommand(sessionsListCmd)
	sessionsCmd.AddCommand(sessionsCloseCmd)

	sessionsListCmd.Flags().StringVar(&sessionsUser, "user", "", "Filter by user name (case-insensitive, partial match)")
	sessionsListCmd.Flags().IntVar(&sessionsLimit, "limit", 0, "Max results to show (default from settings)")
	sessionsListCmd.Flags().BoolVar(&sessionsAll, "all", false, "Show all results, no limit")

	sessionsCloseCmd.Flags().BoolVar(&sessionsCloseYes, "yes", false, "Skip self-close confirmation prompt")
	sessionsCloseCmd.Flags().BoolVar(&sessionsCloseDryRun, "dry-run", false, "Preview the close action without sending it")
}
