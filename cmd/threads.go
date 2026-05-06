package cmd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
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
	threadsUser         string
	threadsState        string
	threadsMinElapsed   string
	threadsLimit        int
	threadsAll          bool
	threadsCancelYes    bool
	threadsCancelDryRun bool
)

var threadsCmd = &cobra.Command{
	Use:   "threads",
	Short: "Manage TM1 server threads",
	Long:  `Manage and inspect threads running on the TM1 server.`,
}

var threadsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List currently running threads",
	Long: `List currently running threads on the TM1 server.

REST API: GET /Threads

Results are limited to 50 by default. Use --all to show all threads.`,
	Example: `  tm1cli threads list
  tm1cli threads list --state Run
  tm1cli threads list --user Admin
  tm1cli threads list --min-elapsed 10s
  tm1cli threads list --all
  tm1cli threads list --output json`,
	RunE: runThreadsList,
}

func runThreadsList(cmd *cobra.Command, args []string) error {
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

	var minElapsed time.Duration
	if threadsMinElapsed != "" {
		d, err := time.ParseDuration(threadsMinElapsed)
		if err != nil {
			output.PrintError(fmt.Sprintf("Invalid --min-elapsed value %q: %s", threadsMinElapsed, err), jsonMode)
			return errSilent
		}
		minElapsed = d
	}

	limit := getLimit(cfg, threadsLimit, threadsAll)
	endpoint := "Threads?$select=ID,Type,Name,Context,State,Function,ObjectType,ObjectName,RLocks,IXLocks,WLocks,ElapsedTime,WaitTime,Info"

	fetchEndpoint := endpoint
	activeFilters := threadsUser != "" || threadsState != "" || threadsMinElapsed != ""
	if limit > 0 && !activeFilters {
		fetchEndpoint += fmt.Sprintf("&$top=%d", limit+100)
	}

	data, err := cl.Get(fetchEndpoint)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var resp model.ThreadResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	threads := filterThreads(resp.Value, threadsUser, threadsState, minElapsed)
	displayThreads(threads, len(threads), limit, jsonMode)
	return nil
}

func filterThreads(threads []model.Thread, user, state string, minElapsed time.Duration) []model.Thread {
	if user == "" && state == "" && minElapsed == 0 {
		return threads
	}
	var out []model.Thread
	for _, t := range threads {
		if user != "" && !strings.Contains(strings.ToLower(t.Name), strings.ToLower(user)) {
			continue
		}
		if state != "" && !strings.EqualFold(t.State, state) {
			continue
		}
		if minElapsed > 0 && float64(t.ElapsedTime) < minElapsed.Seconds() {
			continue
		}
		out = append(out, t)
	}
	return out
}

func formatThreadDuration(d model.ThreadDuration) string {
	secs := float64(d)
	if secs <= 0 {
		return "0ms"
	}
	if secs < 1 {
		return fmt.Sprintf("%dms", int(secs*1000))
	}
	dur := time.Duration(secs * float64(time.Second))
	if dur < time.Minute {
		return fmt.Sprintf("%.1fs", secs)
	}
	h := int(dur.Hours())
	m := int(dur.Minutes()) % 60
	s := int(dur.Seconds()) % 60
	if h > 0 {
		return fmt.Sprintf("%dh%dm%ds", h, m, s)
	}
	return fmt.Sprintf("%dm%ds", m, s)
}

func displayThreads(threads []model.Thread, total int, limit int, jsonMode bool) {
	shown := threads
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}

	if jsonMode {
		output.PrintJSON(shown)
		return
	}

	headers := []string{"ID", "NAME", "TYPE", "STATE", "FUNCTION", "ELAPSED", "WAIT"}
	rows := make([][]string, len(shown))
	for i, t := range shown {
		rows[i] = []string{
			fmt.Sprintf("%d", t.ID),
			t.Name,
			t.Type,
			t.State,
			t.Function,
			formatThreadDuration(t.ElapsedTime),
			formatThreadDuration(t.WaitTime),
		}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(shown), total, "--user, --state, or --min-elapsed to filter or --all")
}

var threadsCancelCmd = &cobra.Command{
	Use:   "cancel <id>",
	Short: "Cancel a running TM1 thread",
	Long: `Cancel a running thread on the TM1 server.

REST API: POST /Threads('<id>')/tm1.CancelOperation

Use this to interrupt a stuck query, hung TI process, or long-running
operation that is holding locks.

WARNING: cancelling a thread aborts in-flight work. Uncommitted writes
are rolled back and any process running on the thread terminates with
an error. Use with care.

The command prompts for confirmation by default. Use --yes to skip the
prompt for scripting.

Use --dry-run to look up the thread and preview what would be cancelled
without actually cancelling it. --dry-run takes precedence over --yes.

Exit codes:
  0  thread cancelled successfully
  1  generic error (auth, network, server)
  3  thread not found`,
	Example: `  tm1cli threads cancel 1234
  tm1cli threads cancel 1234 --yes
  tm1cli threads cancel 1234 --dry-run
  tm1cli threads cancel 1234 --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runThreadsCancel,
}

// threadEndpoint formats a TM1 Threads-collection endpoint with quoted
// key syntax (matches IBM PA REST docs). id must already be validated
// as digits-only at the call site; no escaping is performed.
func threadEndpoint(id, suffix string) string {
	if suffix == "" {
		return fmt.Sprintf("Threads('%s')", id)
	}
	return fmt.Sprintf("Threads('%s')/%s", id, suffix)
}

func runThreadsCancel(cmd *cobra.Command, args []string) error {
	id := args[0]

	cfg, err := loadConfig()
	if err != nil {
		output.PrintError(err.Error(), isJSONOutput(nil))
		return errSilent
	}
	jsonMode := isJSONOutput(cfg)

	if _, err := strconv.ParseUint(id, 10, 64); err != nil {
		output.PrintError(fmt.Sprintf("Invalid thread ID %q (must be numeric).", id), jsonMode)
		return errSilent
	}

	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	if threadsCancelDryRun {
		thread, err := fetchThread(cl, id)
		if err != nil {
			return handleThreadCancelError(err, id, jsonMode)
		}
		if jsonMode {
			output.PrintJSON(map[string]interface{}{
				"status": "dry-run",
				"id":     id,
				"thread": thread,
			})
		} else {
			fmt.Printf("[dry-run] Would cancel thread '%s' (name=%s, state=%s, function=%s, elapsed=%s).\n",
				id, thread.Name, thread.State, thread.Function, formatThreadDuration(thread.ElapsedTime))
		}
		return nil
	}

	if !threadsCancelYes {
		fmt.Fprintf(os.Stderr, "Warning: cancelling thread '%s' aborts in-flight work and may roll back uncommitted writes.\n", id)
		if !promptYesNo(bufio.NewReader(os.Stdin), "Continue?") {
			return nil
		}
	}

	if _, err := cl.Post(threadEndpoint(id, "tm1.CancelOperation"), map[string]interface{}{}); err != nil {
		return handleThreadCancelError(err, id, jsonMode)
	}

	if jsonMode {
		output.PrintJSON(map[string]string{"status": "cancelled", "id": id})
	} else {
		fmt.Printf("Cancelled thread '%s'.\n", id)
	}
	return nil
}

func fetchThread(cl *client.Client, id string) (*model.Thread, error) {
	data, err := cl.Get(threadEndpoint(id, ""))
	if err != nil {
		return nil, err
	}
	var t model.Thread
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("Cannot parse server response.")
	}
	return &t, nil
}

// handleThreadCancelError funnels common failure modes through one place:
// not-found maps to exit code 3 (per the issue), 403 to a friendlier
// message. Falls back to the raw client error otherwise. The 403 detection
// uses the client's "HTTP 403: ..." string contract; if internal/client
// later exposes a typed status error, swap this match for it.
func handleThreadCancelError(err error, id string, jsonMode bool) error {
	if errors.Is(err, client.ErrNotFound) {
		output.PrintError(fmt.Sprintf("Thread '%s' not found.", id), jsonMode)
		return errExit(3)
	}
	if strings.HasPrefix(err.Error(), "HTTP 403") {
		output.PrintError("Permission denied. Cancelling threads requires admin privileges.", jsonMode)
		return errSilent
	}
	output.PrintError(err.Error(), jsonMode)
	return errSilent
}

func init() {
	rootCmd.AddCommand(threadsCmd)
	threadsCmd.AddCommand(threadsListCmd)
	threadsCmd.AddCommand(threadsCancelCmd)

	threadsListCmd.Flags().StringVar(&threadsUser, "user", "", "Filter by thread name/user (case-insensitive, partial match)")
	threadsListCmd.Flags().StringVar(&threadsState, "state", "", "Filter by state (Idle|Run|Wait|CommitWait|Rollback)")
	threadsListCmd.Flags().StringVar(&threadsMinElapsed, "min-elapsed", "", "Minimum elapsed time filter (e.g. 10s, 1m30s)")
	threadsListCmd.Flags().IntVar(&threadsLimit, "limit", 0, "Max results to show (default from settings)")
	threadsListCmd.Flags().BoolVar(&threadsAll, "all", false, "Show all results, no limit")

	threadsCancelCmd.Flags().BoolVar(&threadsCancelYes, "yes", false, "Skip confirmation prompt")
	threadsCancelCmd.Flags().BoolVar(&threadsCancelDryRun, "dry-run", false, "Preview cancel without sending it")
}
