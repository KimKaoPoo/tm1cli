package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"time"
	"tm1cli/internal/client"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

var (
	logsMsgSince    string
	logsMsgLevel    string
	logsMsgUser     string
	logsMsgContains string
	logsMsgFollow   bool
	logsMsgInterval time.Duration
	logsMsgTail     int
	logsMsgRaw      bool
)

var logsCmd = &cobra.Command{
	Use:   "logs",
	Short: "Inspect TM1 server logs",
	Long:  `Inspect TM1 server logs (message log, etc.).`,
}

var logsMessagesCmd = &cobra.Command{
	Use:   "messages",
	Short: "Show and stream the TM1 message log",
	Long: `Show and stream the TM1 message log.

REST API: GET /MessageLogEntries

Filter by time range, severity, user, or message content. Use --follow
to stream new entries kubectl-style; --tail to show the last N entries.

When --follow is combined with --output json, entries are emitted as
NDJSON (one JSON object per line) instead of a JSON array.`,
	Example: `  tm1cli logs messages --tail 50
  tm1cli logs messages --since 10m --level error
  tm1cli logs messages --follow --interval 10s
  tm1cli logs messages --user admin --contains "load"
  tm1cli logs messages --since 2026-04-24T10:00 --raw`,
	RunE: runLogsMessages,
}

// parseSince accepts a duration ("10m") or absolute timestamp and returns RFC3339 UTC.
// Empty input returns "" with no error. Negative duration returns an error.
func parseSince(s string, now time.Time) (string, error) {
	if s == "" {
		return "", nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		if d <= 0 {
			return "", fmt.Errorf("--since duration must be positive (got %q)", s)
		}
		return now.Add(-d).UTC().Format(time.RFC3339), nil
	}
	for _, layout := range []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC().Format(time.RFC3339), nil
		}
	}
	return "", fmt.Errorf("--since value %q is not a duration (e.g. 10m) or timestamp (e.g. 2026-04-24T10:00)", s)
}

// validateLevel accepts a TM1 LogLevel enum case-insensitively.
// Canonical: Info, Warning, Error, Fatal, Debug, Unknown, Off.
// Aliases: warn → Warning, err → Error.
// Empty returns "" with no error.
func validateLevel(s string) (string, error) {
	if s == "" {
		return "", nil
	}
	switch strings.ToLower(s) {
	case "info":
		return "Info", nil
	case "warn", "warning":
		return "Warning", nil
	case "err", "error":
		return "Error", nil
	case "fatal":
		return "Fatal", nil
	case "debug":
		return "Debug", nil
	case "unknown":
		return "Unknown", nil
	case "off":
		return "Off", nil
	default:
		return "", fmt.Errorf("--level must be one of: info, warn, error, fatal, debug, unknown, off (got %q)", s)
	}
}

// parseTimeStamp parses a TM1-style timestamp (RFC3339 with/without Z, optional fractional seconds).
func parseTimeStamp(s string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.999",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamp: %q", s)
}

// buildMessageLogFilter assembles the server-side $filter from sinceTS and level only.
// User is NEVER included — it is handled client-side.
func buildMessageLogFilter(sinceTS, level string) string {
	var parts []string
	if sinceTS != "" {
		parts = append(parts, fmt.Sprintf("TimeStamp ge %s", sinceTS))
	}
	if level != "" {
		parts = append(parts, fmt.Sprintf("Level eq '%s'", level))
	}
	return strings.Join(parts, " and ")
}

// buildMessageLogQuery builds the full endpoint URL path with $filter, $top, and $orderby
// using url.Values for safe encoding.
func buildMessageLogQuery(filter string, top int, orderDesc bool) string {
	v := url.Values{}
	if filter != "" {
		v.Set("$filter", filter)
	}
	if top > 0 {
		v.Set("$top", strconv.Itoa(top))
	}
	if orderDesc {
		v.Set("$orderby", "TimeStamp desc")
	}
	if len(v) == 0 {
		return "MessageLogEntries"
	}
	return "MessageLogEntries?" + v.Encode()
}

// isFilterRejection returns true for HTTP 400/501 with a body referencing
// filter/orderby/not-supported keywords.
// Auth errors (401/403) and not-found (404) are NOT filter rejections.
func isFilterRejection(err error) bool {
	if errors.Is(err, client.ErrNotFound) {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "Authentication failed") {
		return false
	}
	if strings.HasPrefix(msg, "HTTP 400") || strings.HasPrefix(msg, "HTTP 501") {
		lower := strings.ToLower(msg)
		return strings.Contains(lower, "filter") ||
			strings.Contains(lower, "$filter") ||
			strings.Contains(lower, "orderby") ||
			strings.Contains(lower, "not supported") ||
			strings.Contains(lower, "not implemented")
	}
	return false
}

// fetchMessageLogEntries performs GET. On HTTP 400/501 with a filter-rejection body,
// it retries without $filter (preserving $top and $orderby) and returns fallback=true.
// On auth (401/403), not-found (404), or network errors the error propagates unchanged.
func fetchMessageLogEntries(cl *client.Client, filter string, top int, orderDesc bool) ([]model.MessageLogEntry, bool, error) {
	endpoint := buildMessageLogQuery(filter, top, orderDesc)
	data, err := cl.Get(endpoint)
	if err != nil {
		if filter != "" && isFilterRejection(err) {
			retryEndpoint := buildMessageLogQuery("", top, orderDesc)
			retryData, retryErr := cl.Get(retryEndpoint)
			if retryErr != nil {
				return nil, false, retryErr
			}
			var resp model.MessageLogResponse
			if jsonErr := json.Unmarshal(retryData, &resp); jsonErr != nil {
				return nil, false, fmt.Errorf("cannot parse server response: %w", jsonErr)
			}
			output.PrintWarning("Server-side filter not supported, filtering locally...")
			return resp.Value, true, nil
		}
		return nil, false, err
	}
	var resp model.MessageLogResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, false, fmt.Errorf("cannot parse server response: %w", err)
	}
	return resp.Value, false, nil
}

// applyClientFilters applies user/contains filters always; on fallback also applies since/level.
// User filter matches entry.User if present, else searches entry.Message (case-insensitive).
func applyClientFilters(entries []model.MessageLogEntry, sinceTS, level, user, contains string) []model.MessageLogEntry {
	var sinceT time.Time
	if sinceTS != "" {
		sinceT, _ = parseTimeStamp(sinceTS)
	}
	levelLower := strings.ToLower(level)
	uLower := strings.ToLower(user)
	cLower := strings.ToLower(contains)

	out := make([]model.MessageLogEntry, 0, len(entries))
	for _, e := range entries {
		if !sinceT.IsZero() {
			t, err := parseTimeStamp(e.TimeStamp)
			if err != nil || t.Before(sinceT) {
				continue
			}
		}
		if level != "" && strings.ToLower(e.Level) != levelLower {
			continue
		}
		var msgLower string
		needsMsg := contains != "" || (user != "" && e.User == "")
		if needsMsg {
			msgLower = strings.ToLower(e.Message)
		}
		if user != "" {
			matched := false
			if e.User != "" {
				matched = strings.Contains(strings.ToLower(e.User), uLower)
			} else {
				matched = strings.Contains(msgLower, uLower)
			}
			if !matched {
				continue
			}
		}
		if contains != "" && !strings.Contains(msgLower, cLower) {
			continue
		}
		out = append(out, e)
	}
	return out
}

// sortEntriesByTimeStamp sorts ascending; ties are broken by ID. Unparseable timestamps go to the end stably.
func sortEntriesByTimeStamp(entries []model.MessageLogEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		ti, errI := parseTimeStamp(entries[i].TimeStamp)
		tj, errJ := parseTimeStamp(entries[j].TimeStamp)
		switch {
		case errI != nil && errJ != nil:
			return false
		case errI != nil:
			return false
		case errJ != nil:
			return true
		case ti.Equal(tj):
			return entries[i].ID < entries[j].ID
		default:
			return ti.Before(tj)
		}
	})
}

// reverseEntries reverses entries in place.
func reverseEntries(entries []model.MessageLogEntry) {
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
}

// boundaryIDs returns the set of IDs whose TimeStamp equals the max in entries,
// and the max TimeStamp string itself. Used as the dedupe set for --follow.
func boundaryIDs(entries []model.MessageLogEntry) (string, map[string]struct{}) {
	if len(entries) == 0 {
		return "", nil
	}
	maxT, errFirst := parseTimeStamp(entries[0].TimeStamp)
	hasMaxT := errFirst == nil
	maxTS := entries[0].TimeStamp
	for _, e := range entries[1:] {
		t, err := parseTimeStamp(e.TimeStamp)
		if err != nil {
			continue
		}
		if !hasMaxT || t.After(maxT) {
			maxT, maxTS, hasMaxT = t, e.TimeStamp, true
		}
	}
	ids := map[string]struct{}{}
	for _, e := range entries {
		if e.TimeStamp == maxTS && e.ID != "" {
			ids[e.ID] = struct{}{}
		}
	}
	return maxTS, ids
}

// defaultTailIfUnbounded returns 100 when no time-bound flag is set to avoid
// unbounded reads against multi-GB message logs. Applies in --follow too:
// kubectl-style, show the last N entries first then stream new ones.
func defaultTailIfUnbounded(since string, tail int) int {
	if since == "" && tail == 0 {
		return 100
	}
	return tail
}

// resolveFollowWatermark picks the starting watermark for --follow polls.
// Falls back to now (rather than empty) when no entries were seen and no
// --since was given, so the first poll's $filter is always bounded — without
// it, GET /MessageLogEntries would have neither $filter nor $top and could
// return the entire message log on every tick.
func resolveFollowWatermark(maxTS, sinceTS string, now time.Time) string {
	if maxTS != "" {
		return maxTS
	}
	if sinceTS != "" {
		return sinceTS
	}
	return now.UTC().Format(time.RFC3339)
}

// rawMessageReplacer collapses embedded \r\n, \n, \r, \t to single spaces
// so raw output keeps its one-line-per-entry guarantee.
var rawMessageReplacer = strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ", "\t", " ")

func sanitizeRawMessage(msg string) string {
	return rawMessageReplacer.Replace(msg)
}

// printMessageLogEntries renders entries in the chosen format.
// In follow+jsonMode it emits NDJSON (one object per line).
// In raw mode it emits one sanitized line per entry.
func printMessageLogEntries(entries []model.MessageLogEntry, jsonMode, rawMode, isFollowChunk bool) {
	if jsonMode {
		if isFollowChunk {
			for _, e := range entries {
				data, _ := json.Marshal(e)
				fmt.Println(string(data))
			}
			return
		}
		output.PrintJSON(entries)
		return
	}
	if rawMode {
		for _, e := range entries {
			user := e.User
			if user == "" {
				user = "-"
			}
			fmt.Printf("%s %s [%s] %s\n", e.TimeStamp, e.Level, user, sanitizeRawMessage(e.Message))
		}
		return
	}
	headers := []string{"TIME", "LEVEL", "USER", "LOGGER", "MESSAGE"}
	rows := make([][]string, len(entries))
	for i, e := range entries {
		rows[i] = []string{e.TimeStamp, e.Level, e.User, e.Logger, sanitizeRawMessage(e.Message)}
	}
	output.PrintTable(headers, rows)
}

// followMessageLogs is the extracted polling loop driven by ctx for testability.
func followMessageLogs(ctx context.Context, cl *client.Client, watermarkTS string, watermarkIDs map[string]struct{}, level, user, contains string, interval time.Duration, jsonMode, rawMode bool) error {
	if watermarkIDs == nil {
		watermarkIDs = map[string]struct{}{}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}

		filter := buildMessageLogFilter(watermarkTS, level)
		entries, fallback, err := fetchMessageLogEntries(cl, filter, 0, false)
		if err != nil {
			output.PrintWarning(fmt.Sprintf("poll failed: %s", err))
			continue
		}

		// Drop entries already shown at the boundary (clock-skew dedupe).
		filtered := entries[:0]
		for _, e := range entries {
			if e.ID != "" {
				if _, seen := watermarkIDs[e.ID]; seen {
					continue
				}
			}
			filtered = append(filtered, e)
		}
		entries = filtered

		applySince, applyLevel := "", ""
		if fallback {
			applySince, applyLevel = watermarkTS, level
		}
		if applySince != "" || applyLevel != "" || user != "" || contains != "" {
			entries = applyClientFilters(entries, applySince, applyLevel, user, contains)
		}
		if len(entries) == 0 {
			continue
		}

		sortEntriesByTimeStamp(entries)
		printMessageLogEntries(entries, jsonMode, rawMode, true)

		newMaxTS, newIDs := boundaryIDs(entries)
		if newMaxTS != "" {
			watermarkTS, watermarkIDs = advanceWatermark(newMaxTS, newIDs)
		}
	}
}

// advanceWatermark returns the next polling watermark + dedupe set. When the
// boundary entries lack IDs (older TM1 versions), advance past the boundary
// by 1ms so the inclusive `TimeStamp ge` filter doesn't re-fetch them — we
// have nothing to dedupe them against. With sub-second resolution rare in
// TM1 message logs, this is unlikely to skip real entries.
func advanceWatermark(maxTS string, ids map[string]struct{}) (string, map[string]struct{}) {
	if len(ids) > 0 {
		return maxTS, ids
	}
	t, err := parseTimeStamp(maxTS)
	if err != nil {
		return maxTS, map[string]struct{}{}
	}
	return t.Add(time.Millisecond).UTC().Format(time.RFC3339Nano), map[string]struct{}{}
}

func runLogsMessages(cmd *cobra.Command, args []string) error {
	cfg, err := loadConfig()
	if err != nil {
		output.PrintError(err.Error(), isJSONOutput(nil))
		return errSilent
	}
	jsonMode := isJSONOutput(cfg)

	if logsMsgRaw && jsonMode {
		output.PrintError("--raw cannot be combined with --output json.", false)
		return errSilent
	}

	if logsMsgTail < 0 {
		output.PrintError("--tail must be non-negative.", jsonMode)
		return errSilent
	}

	if logsMsgFollow && logsMsgInterval <= 0 {
		output.PrintError("--interval must be greater than zero (e.g. 5s).", jsonMode)
		return errSilent
	}

	level, err := validateLevel(logsMsgLevel)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}
	sinceTS, err := parseSince(logsMsgSince, time.Now())
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	tail := defaultTailIfUnbounded(logsMsgSince, logsMsgTail)

	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	filter := buildMessageLogFilter(sinceTS, level)
	entries, fallback, err := fetchMessageLogEntries(cl, filter, tail, tail > 0)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	applySince, applyLevel := "", ""
	if fallback {
		applySince, applyLevel = sinceTS, level
	}
	if applySince != "" || applyLevel != "" || logsMsgUser != "" || logsMsgContains != "" {
		entries = applyClientFilters(entries, applySince, applyLevel, logsMsgUser, logsMsgContains)
	}

	if tail > 0 {
		reverseEntries(entries)
	} else {
		sortEntriesByTimeStamp(entries)
	}

	printMessageLogEntries(entries, jsonMode, logsMsgRaw, false)

	if !logsMsgFollow {
		return nil
	}

	maxTS, ids := boundaryIDs(entries)
	maxTS = resolveFollowWatermark(maxTS, sinceTS, time.Now())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	return followMessageLogs(ctx, cl, maxTS, ids, level, logsMsgUser, logsMsgContains, logsMsgInterval, jsonMode, logsMsgRaw)
}

func init() {
	rootCmd.AddCommand(logsCmd)
	logsCmd.AddCommand(logsMessagesCmd)

	logsMessagesCmd.Flags().StringVar(&logsMsgSince, "since", "", "Show entries newer than duration (e.g. 10m, 2h) or timestamp (e.g. 2026-04-24T10:00)")
	logsMessagesCmd.Flags().StringVar(&logsMsgLevel, "level", "", "Filter by level: info, warn, error, fatal, debug, unknown, off")
	logsMessagesCmd.Flags().StringVar(&logsMsgUser, "user", "", "Filter by user (client-side; matches User field or message text)")
	logsMessagesCmd.Flags().StringVar(&logsMsgContains, "contains", "", "Filter by substring in Message (case-insensitive)")
	logsMessagesCmd.Flags().BoolVarP(&logsMsgFollow, "follow", "f", false, "Stream new entries kubectl-style (--output json emits NDJSON)")
	logsMessagesCmd.Flags().DurationVar(&logsMsgInterval, "interval", 5*time.Second, "Polling interval when --follow is set")
	logsMessagesCmd.Flags().IntVar(&logsMsgTail, "tail", 0, "Show last N entries (defaults to 100 when no --since/--follow)")
	logsMessagesCmd.Flags().BoolVar(&logsMsgRaw, "raw", false, "Raw output: one line per entry")
}
