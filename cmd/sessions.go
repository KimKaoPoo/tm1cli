package cmd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"tm1cli/internal/client"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

var (
	sessionsUser        string
	sessionsInactiveFor string
	sessionsLimit       int
	sessionsAll         bool
	sessionsCloseYes    bool
	sessionsCloseDryRun bool
)

const (
	sessionsListBase     = "Sessions?$select=ID,Context,Active,LastActivity&$expand=User($select=Name)"
	sessionsThreadsParam = ",Threads($select=ID)"
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

Results are limited to 50 by default. Use --all to show all sessions.

Filter --inactive-for accepts a Go duration (e.g. 30m, 1h, 1h30m) and
keeps only sessions whose LastActivity is at least that old.`,
	Example: `  tm1cli sessions list
  tm1cli sessions list --user admin
  tm1cli sessions list --inactive-for 1h
  tm1cli sessions list --all
  tm1cli sessions list --output json`,
	RunE: runSessionsList,
}

var sessionsCloseCmd = &cobra.Command{
	Use:   "close <id>",
	Short: "Close an active session",
	Long: `Close an active session on the TM1 server.

REST API: POST /Sessions('<id>')/tm1.Close

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

	var inactiveFor time.Duration
	if sessionsInactiveFor != "" {
		d, err := time.ParseDuration(sessionsInactiveFor)
		if err != nil {
			output.PrintError(fmt.Sprintf("Invalid --inactive-for value %q: %s", sessionsInactiveFor, err), jsonMode)
			return errSilent
		}
		if d < 0 {
			output.PrintError(fmt.Sprintf("--inactive-for duration must be non-negative (got %q)", sessionsInactiveFor), jsonMode)
			return errSilent
		}
		inactiveFor = d
	}

	limit := getLimit(cfg, sessionsLimit, sessionsAll)
	activeFilters := sessionsUser != "" || sessionsInactiveFor != ""

	sessions, threadsAvailable, err := fetchSessions(cl, limit, activeFilters)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	now := time.Now()
	filtered := filterSessions(sessions, sessionsUser, inactiveFor, now)
	displaySessions(filtered, len(filtered), limit, jsonMode, threadsAvailable, now)
	return nil
}

// fetchSessions issues GET /Sessions with the full $expand. If the server
// rejects $expand=Threads (HTTP 400/501), it retries without that clause
// and returns threadsAvailable=false plus a warning.
func fetchSessions(cl *client.Client, limit int, activeFilters bool) ([]model.Session, bool, error) {
	endpoint := sessionsListBase + sessionsThreadsParam
	if limit > 0 && !activeFilters {
		endpoint += fmt.Sprintf("&$top=%d", limit+100)
	}

	data, err := cl.Get(endpoint)
	if err != nil {
		if !isExpandRejection(err) {
			return nil, false, err
		}
		retryEndpoint := sessionsListBase
		if limit > 0 && !activeFilters {
			retryEndpoint += fmt.Sprintf("&$top=%d", limit+100)
		}
		retryData, retryErr := cl.Get(retryEndpoint)
		if retryErr != nil {
			return nil, false, err
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

// isExpandRejection reports whether err looks like a server-side rejection
// of an $expand clause: HTTP 400 or 501 that is not an auth or not-found
// error. Used to gate the Threads-expand fallback retry.
func isExpandRejection(err error) bool {
	if err == nil || errors.Is(err, client.ErrNotFound) {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "Authentication failed") {
		return false
	}
	if strings.HasPrefix(msg, "HTTP 403") {
		return false
	}
	return strings.HasPrefix(msg, "HTTP 400") || strings.HasPrefix(msg, "HTTP 501")
}

func filterSessions(sessions []model.Session, user string, inactiveFor time.Duration, now time.Time) []model.Session {
	if user == "" && inactiveFor == 0 {
		return sessions
	}
	var out []model.Session
	for _, s := range sessions {
		if user != "" && !strings.Contains(strings.ToLower(s.User.Name), strings.ToLower(user)) {
			continue
		}
		if inactiveFor > 0 {
			t, ok := parseSessionTime(s.LastActivity)
			if ok && now.Sub(t) < inactiveFor {
				continue
			}
		}
		out = append(out, s)
	}
	return out
}

// parseSessionTime accepts RFC3339 with or without fractional seconds. It
// returns ok=false on empty or unparseable input so callers can keep the
// entry as "unknown age" instead of silently dropping it.
func parseSessionTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// formatLastActivity renders LastActivity relative to now: "5m ago" /
// "2h ago" / "3d ago" or absolute UTC for ages over a week. Empty input
// returns "" and unparseable input is echoed verbatim.
func formatLastActivity(s string, now time.Time) string {
	if s == "" {
		return ""
	}
	t, ok := parseSessionTime(s)
	if !ok {
		return s
	}
	d := now.Sub(t)
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	case d < 7*24*time.Hour:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	default:
		return t.UTC().Format("2006-01-02 15:04")
	}
}

func displaySessions(sessions []model.Session, total int, limit int, jsonMode bool, threadsAvailable bool, now time.Time) {
	shown := sessions
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}

	if jsonMode {
		output.PrintJSON(shown)
		return
	}

	headers := []string{"ID", "USER", "CONTEXT", "ACTIVE", "LAST ACTIVITY", "THREADS"}
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
			formatLastActivity(s.LastActivity, now),
			threadsCell,
		}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(shown), total, "--user, --inactive-for to filter or --all")
}

func runSessionsClose(cmd *cobra.Command, args []string) error {
	id := args[0]

	cfg, err := loadConfig()
	if err != nil {
		output.PrintError(err.Error(), isJSONOutput(nil))
		return errSilent
	}

	jsonMode := isJSONOutput(cfg)

	if !isAllDigits(id) {
		output.PrintError(fmt.Sprintf("Invalid session ID %q (must be numeric).", id), jsonMode)
		return errSilent
	}

	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	isSelfClose, selfErr := detectSelfClose(cl, id)
	if selfErr != nil && flagVerbose {
		fmt.Fprintf(os.Stderr, "[verbose] could not look up active session: %s\n", selfErr)
	}

	if isSelfClose && !sessionsCloseYes {
		fmt.Fprintf(os.Stderr, "Warning: '%s' is your active session. Closing it will log you out.\n", id)
		if !promptYesNo(bufio.NewReader(os.Stdin), "Continue?") {
			return nil
		}
	}

	if sessionsCloseDryRun {
		if jsonMode {
			output.PrintJSON(map[string]string{"status": "dry-run", "id": id})
		} else {
			fmt.Printf("[dry-run] Would close session '%s'.\n", id)
		}
		return nil
	}

	endpoint := fmt.Sprintf("Sessions('%s')/tm1.Close", url.PathEscape(id))
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
// /ActiveSession and reports whether its ID equals target. Errors are
// returned to the caller, which treats them as best-effort (no warning).
func detectSelfClose(cl *client.Client, target string) (bool, error) {
	data, err := cl.Get("ActiveSession?$select=ID")
	if err != nil {
		return false, err
	}
	var ref model.ActiveSessionRef
	if err := json.Unmarshal(data, &ref); err != nil {
		return false, fmt.Errorf("cannot parse ActiveSession response")
	}
	return strconv.FormatInt(ref.ID, 10) == target, nil
}

// isAllDigits reports whether s consists only of ASCII digits and is non-empty.
func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func init() {
	rootCmd.AddCommand(sessionsCmd)
	sessionsCmd.AddCommand(sessionsListCmd)
	sessionsCmd.AddCommand(sessionsCloseCmd)

	sessionsListCmd.Flags().StringVar(&sessionsUser, "user", "", "Filter by user name (case-insensitive, partial match)")
	sessionsListCmd.Flags().StringVar(&sessionsInactiveFor, "inactive-for", "", "Only show sessions inactive for at least this duration (e.g. 30m, 1h)")
	sessionsListCmd.Flags().IntVar(&sessionsLimit, "limit", 0, "Max results to show (default from settings)")
	sessionsListCmd.Flags().BoolVar(&sessionsAll, "all", false, "Show all results, no limit")

	sessionsCloseCmd.Flags().BoolVar(&sessionsCloseYes, "yes", false, "Skip self-close confirmation prompt")
	sessionsCloseCmd.Flags().BoolVar(&sessionsCloseDryRun, "dry-run", false, "Preview the close action without sending it")
}
