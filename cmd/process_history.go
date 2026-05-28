package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
	"tm1cli/internal/client"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

// processHistoryStatusError is the status reported for every run surfaced by
// the ErrorLogs endpoint — by definition those are failed/errored runs.
const processHistoryStatusError = "Error"

var (
	procHistTail         int
	procHistSince        string
	procHistOnlyFailures bool
	procHistShowError    string
)

var processHistoryCmd = &cobra.Command{
	Use:   "history <name>",
	Short: "Show recent runs (error logs) of a TI process",
	Long: `Show recent runs of a TI process from its error-log files.

REST API: GET /ErrorLogFiles (filtered client-side to this process by the
canonical TM1ProcessError_<timestamp>_<name>.log filename pattern).

TM1 records each failed/aborted process run as an error-log file on the
server. This source needs no audit/message logging, so it works even when
those are disabled. The endpoint does NOT expose the user or duration of a
run, so those columns show "-", and every row's status is "Error" — for
that reason --only-failures is effectively a compatibility no-op here.

Use --show-error <logfile> to dump the full content of one error-log file
listed in the history (via GET /ErrorLogFiles('<file>')/Content). When set,
it takes precedence over the listing flags.`,
	Example: `  tm1cli process history "LoadData"
  tm1cli process history "LoadData" --tail 10
  tm1cli process history "LoadData" --since 24h
  tm1cli process history "LoadData" --since 2026-05-28T10:00
  tm1cli process history "LoadData" --only-failures
  tm1cli process history "LoadData" --show-error TM1ProcessError_20260528_LoadData.log
  tm1cli process history "LoadData" --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runProcessHistory,
}

// errorLogFilesEndpoint is the global error-log-file collection. Per-process
// filtering happens client-side via the canonical filename pattern (see
// parseErrorLogFilename), because the per-process Processes('name')/ErrorLogs
// endpoint is keyed by Timestamp and does not carry filenames.
func errorLogFilesEndpoint() string {
	return "ErrorLogFiles"
}

// errorLogContentEndpoint builds the path to a single error-log file's content.
// odataKey doubles single quotes before URL-escaping so filenames containing '
// produce a valid OData string literal.
func errorLogContentEndpoint(file string) string {
	return fmt.Sprintf("ErrorLogFiles('%s')/Content", odataKey(file))
}

// parseErrorLogFilename pulls the process name and start time from a TM1
// process error-log filename. The canonical shape is
// TM1ProcessError_<14-digit-YYYYMMDDHHMMSS>_<processname>.log.
//
// Returns ok=true when the structural shape matches (regardless of whether
// the timestamp slot is parseable). startTime is empty when the 14-char slot
// is not a valid YYYYMMDDHHMMSS — the table renders that as "-".
func parseErrorLogFilename(filename string) (procName, startTime string, ok bool) {
	const prefix = "TM1ProcessError_"
	const suffix = ".log"
	const tsLen = 14
	if !strings.HasPrefix(filename, prefix) || !strings.HasSuffix(filename, suffix) {
		return "", "", false
	}
	rest := filename[len(prefix) : len(filename)-len(suffix)]
	// Need at least: 14-char timestamp slot + '_' + at least 1 char of name.
	if len(rest) < tsLen+2 || rest[tsLen] != '_' {
		return "", "", false
	}
	name := rest[tsLen+1:]
	if t, err := time.Parse("20060102150405", rest[:tsLen]); err == nil {
		startTime = t.UTC().Format(time.RFC3339)
	}
	return name, startTime, true
}

// buildHistoryEntries filters the global error-log-file collection to those
// belonging to processName (by canonical filename pattern) and maps them to
// display rows. Returns a non-nil slice so JSON output renders [] not null.
// User and Duration are left empty — the ErrorLogFiles endpoint does not
// expose them.
func buildHistoryEntries(files []model.ErrorLogFile, processName string) []model.ProcessHistoryEntry {
	out := make([]model.ProcessHistoryEntry, 0, len(files))
	for _, f := range files {
		filename := f.LogFile()
		name, startTime, ok := parseErrorLogFilename(filename)
		if !ok || name != processName {
			continue
		}
		out = append(out, model.ProcessHistoryEntry{
			StartTime:    startTime,
			Status:       processHistoryStatusError,
			ErrorLogFile: filename,
		})
	}
	return out
}

// filterHistorySince keeps entries whose StartTime is at or after sinceTS
// (RFC3339 UTC). Empty sinceTS returns entries unchanged. Entries with an
// unparseable StartTime are dropped when a since bound is set, matching the
// logs.go message-log filtering semantics.
func filterHistorySince(entries []model.ProcessHistoryEntry, sinceTS string) []model.ProcessHistoryEntry {
	if sinceTS == "" {
		return entries
	}
	cutoff, err := parseTimeStamp(sinceTS)
	if err != nil {
		return entries
	}
	out := make([]model.ProcessHistoryEntry, 0, len(entries))
	for _, e := range entries {
		t, err := parseTimeStamp(e.StartTime)
		if err != nil || t.Before(cutoff) {
			continue
		}
		out = append(out, e)
	}
	return out
}

// isFailureStatus reports whether status denotes a failed run. Every row from
// the ErrorLogs endpoint is "Error"; the predicate is written generically
// (anything not success/completed) so it stays correct if richer statuses are
// ever introduced.
func isFailureStatus(status string) bool {
	return !strings.EqualFold(status, "success") && !strings.EqualFold(status, "completed")
}

// filterOnlyFailures keeps only failed runs.
func filterOnlyFailures(entries []model.ProcessHistoryEntry) []model.ProcessHistoryEntry {
	out := make([]model.ProcessHistoryEntry, 0, len(entries))
	for _, e := range entries {
		if isFailureStatus(e.Status) {
			out = append(out, e)
		}
	}
	return out
}

// sortHistoryByTimeDesc sorts most-recent-first; entries with an unparseable
// StartTime sort to the end, stably.
func sortHistoryByTimeDesc(entries []model.ProcessHistoryEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		ti, ei := parseTimeStamp(entries[i].StartTime)
		tj, ej := parseTimeStamp(entries[j].StartTime)
		switch {
		case ei != nil && ej != nil:
			return false
		case ei != nil:
			return false
		case ej != nil:
			return true
		default:
			return ti.After(tj)
		}
	})
}

// tailHistory keeps the first n entries (the most recent, after a desc sort).
// n <= 0 returns all entries.
func tailHistory(entries []model.ProcessHistoryEntry, n int) []model.ProcessHistoryEntry {
	if n > 0 && len(entries) > n {
		return entries[:n]
	}
	return entries
}

// orDash returns "-" for an empty string, else the string unchanged.
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func runProcessHistory(cmd *cobra.Command, args []string) error {
	processName := args[0]

	cfg, err := loadConfig()
	if err != nil {
		output.PrintError(err.Error(), isJSONOutput(nil))
		return errSilent
	}

	jsonMode := isJSONOutput(cfg)

	if procHistTail < 0 {
		output.PrintError("--tail must be non-negative.", jsonMode)
		return errSilent
	}

	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	// --show-error takes precedence: dump one file's content, ignore listing flags.
	if procHistShowError != "" {
		return runShowError(cl, procHistShowError, jsonMode)
	}

	sinceTS, err := parseSince(procHistSince, time.Now())
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	data, err := cl.Get(errorLogFilesEndpoint())
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var resp model.ErrorLogFileResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	entries := buildHistoryEntries(resp.Value, processName)
	rawCount := len(entries)
	entries = filterHistorySince(entries, sinceTS)
	if procHistOnlyFailures {
		entries = filterOnlyFailures(entries)
	}
	sortHistoryByTimeDesc(entries)
	total := len(entries)
	entries = tailHistory(entries, procHistTail)

	displayProcessHistory(processName, entries, total, rawCount, jsonMode)
	return nil
}

// displayProcessHistory renders the run history. total is the count after
// filtering; rawCount is the count before filtering, used only to distinguish
// "no history at all" from "history exists but nothing matched the filters".
func displayProcessHistory(name string, entries []model.ProcessHistoryEntry, total, rawCount int, jsonMode bool) {
	if jsonMode {
		if entries == nil {
			entries = []model.ProcessHistoryEntry{}
		}
		output.PrintJSON(entries)
		return
	}

	if total == 0 {
		if rawCount == 0 {
			fmt.Fprintf(os.Stderr, "No error-log history found for process '%s'.\n", name)
		} else {
			fmt.Fprintf(os.Stderr, "No runs match the filter for process '%s'.\n", name)
		}
		return
	}

	headers := []string{"START TIME", "STATUS", "USER", "DURATION", "ERROR LOG FILE"}
	rows := make([][]string, len(entries))
	for i, e := range entries {
		rows[i] = []string{e.StartTime, e.Status, orDash(e.User), orDash(e.Duration), orDash(e.ErrorLogFile)}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(entries), total, "--tail 0")
}

// runShowError fetches and prints the content of a single error-log file.
func runShowError(cl *client.Client, file string, jsonMode bool) error {
	data, err := cl.Get(errorLogContentEndpoint(file))
	if err != nil {
		if errors.Is(err, client.ErrNotFound) {
			output.PrintError(fmt.Sprintf("Error log file '%s' not found.", file), jsonMode)
			return errExit(3)
		}
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	if jsonMode {
		output.PrintJSON(map[string]string{"file": file, "content": string(data)})
		return nil
	}

	os.Stdout.Write(data)
	if len(data) > 0 && data[len(data)-1] != '\n' {
		fmt.Println()
	}
	return nil
}

func init() {
	processCmd.AddCommand(processHistoryCmd)

	processHistoryCmd.Flags().IntVar(&procHistTail, "tail", 0, "Show only the last N runs, most recent first (0 = all)")
	processHistoryCmd.Flags().StringVar(&procHistSince, "since", "", "Show runs newer than duration (e.g. 24h) or timestamp (e.g. 2026-05-28T10:00 — local time when no zone given)")
	processHistoryCmd.Flags().BoolVar(&procHistOnlyFailures, "only-failures", false, "Show only failed runs (compatibility no-op: all error-log entries are failures)")
	processHistoryCmd.Flags().StringVar(&procHistShowError, "show-error", "", "Dump the content of a specific error-log file instead of listing history")
}
