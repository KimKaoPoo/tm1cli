package cmd

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

var (
	threadsUser       string
	threadsState      string
	threadsMinElapsed string
	threadsLimit      int
	threadsAll        bool
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

	data, err := cl.Get(endpoint)
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
	if secs < 1 {
		return fmt.Sprintf("%dms", int(secs*1000))
	}
	dur := time.Duration(secs * float64(time.Second))
	if dur < time.Minute {
		return fmt.Sprintf("%.1fs", secs)
	}
	m := int(dur.Minutes())
	s := int(dur.Seconds()) % 60
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
	output.PrintSummary(len(shown), total)
}

func init() {
	rootCmd.AddCommand(threadsCmd)
	threadsCmd.AddCommand(threadsListCmd)

	threadsListCmd.Flags().StringVar(&threadsUser, "user", "", "Filter by thread name/user (case-insensitive, partial match)")
	threadsListCmd.Flags().StringVar(&threadsState, "state", "", "Filter by state (Idle|Run|Wait|CommitWait|Rollback)")
	threadsListCmd.Flags().StringVar(&threadsMinElapsed, "min-elapsed", "", "Minimum elapsed time filter (e.g. 10s, 1m30s)")
	threadsListCmd.Flags().IntVar(&threadsLimit, "limit", 0, "Max results to show (default from settings)")
	threadsListCmd.Flags().BoolVar(&threadsAll, "all", false, "Show all results, no limit")
}
