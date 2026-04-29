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
	logsTxSince    string
	logsTxUntil    string
	logsTxCube     string
	logsTxUser     string
	logsTxFollow   bool
	logsTxInterval time.Duration
	logsTxTail     int
	logsTxRaw      bool
)

var logsTxCmd = &cobra.Command{
	Use:   "tx",
	Short: "Show and stream the TM1 transaction log",
	Long: `Show and stream the TM1 transaction log (cell-value changes).

REST API: GET /TransactionLogEntries

Filter by time range, cube, or user. Use --follow to stream new entries
kubectl-style; --tail to show the last N entries.

--cube and --user are matched case-sensitively (server-side OData eq); when
the server cannot apply $filter, the client falls back to the same
case-sensitive equality.

When --follow is combined with --output json, entries are emitted as NDJSON
(one JSON object per line) instead of a JSON array.`,
	Example: `  tm1cli logs tx --tail 50
  tm1cli logs tx --since 10m --cube Sales
  tm1cli logs tx --user admin --follow
  tm1cli logs tx --since 2026-04-24T10:00 --until 2026-04-24T18:00 --raw`,
	RunE: runLogsTx,
}

// odataEscape doubles embedded single quotes per OData v4 string literal rules.
func odataEscape(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// parseTimeFlag wraps parseSince so error messages name the originating flag
// (e.g. "--until value ..." instead of "--since value ...").
func parseTimeFlag(name, value string, now time.Time) (string, error) {
	ts, err := parseSince(value, now)
	if err != nil {
		return "", errors.New(strings.Replace(err.Error(), "--since", name, 1))
	}
	return ts, nil
}

// formatTxValue renders a TM1 Edm.PrimitiveType value (string, number, bool,
// or null) for table/raw output. JSON strings are unquoted; everything else
// is rendered as the trimmed raw bytes; null and empty become "". When raw
// looks like a JSON string but Unmarshal fails (malformed escape, etc.), we
// fall through to the trimmed raw bytes — surfacing the corruption rather
// than swallowing it with an error column.
func formatTxValue(raw json.RawMessage) string {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return ""
	}
	if strings.HasPrefix(trimmed, "\"") {
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			return s
		}
	}
	return trimmed
}

// txIDKey returns the dedupe key for a transaction-log entry. When TM1
// supplies ID we use it directly. When ID is omitted (older versions) we
// synthesize a key from the observable fields so same-timestamp entries
// can be distinguished — important for tx logs where batch writes / TI
// processes commonly emit multiple changes at the same exact timestamp.
// Two functionally identical events (same content, same timestamp) hash
// to the same key, which is the right behavior — they are indistinguishable.
func txIDKey(e model.TransactionLogEntry) string {
	if e.ID != 0 {
		return strconv.FormatInt(e.ID, 10)
	}
	// 0x1f (unit separator) is illegal in TM1 names and OData strings, so
	// it cannot appear inside any field — safe as a delimiter.
	const sep = "\x1f"
	return strings.Join([]string{
		"syn",
		e.TimeStamp,
		e.User,
		e.Cube,
		strings.Join(e.Tuple, sep),
		string(e.OldValue),
		string(e.NewValue),
		e.StatusMessage,
	}, sep)
}

// tupleString renders a TM1 cell coordinate as colon-joined elements, matching
// the cellset coord convention used elsewhere in the project.
func tupleString(t []string) string {
	return strings.Join(t, ":")
}

// buildTxFilter assembles the OData $filter from time bounds and exact-match
// cube/user. Empty inputs are skipped. User and Cube use OData eq with
// odata-escaped string literals.
func buildTxFilter(sinceTS, untilTS, cube, user string) string {
	var parts []string
	if sinceTS != "" {
		parts = append(parts, fmt.Sprintf("TimeStamp ge %s", sinceTS))
	}
	if untilTS != "" {
		parts = append(parts, fmt.Sprintf("TimeStamp le %s", untilTS))
	}
	if cube != "" {
		parts = append(parts, fmt.Sprintf("Cube eq '%s'", odataEscape(cube)))
	}
	if user != "" {
		parts = append(parts, fmt.Sprintf("User eq '%s'", odataEscape(user)))
	}
	return strings.Join(parts, " and ")
}

// buildTxQuery builds the endpoint URL with $filter, $top, $orderby using
// url.Values for safe encoding.
func buildTxQuery(filter string, top int, orderDesc bool) string {
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
		return "TransactionLogEntries"
	}
	return "TransactionLogEntries?" + v.Encode()
}

// fetchTxEntries performs GET. On HTTP 400/501 with a filter-rejection body,
// it retries without $filter (over-fetching via fallbackRetryTop when --tail
// is set so client-side filtering has room to find matches) and returns
// fallback=true. On auth/not-found/network errors the error propagates unchanged.
//
// sinceSet/untilSet steer the retry order so the cap captures the right slice
// of the log:
//   - --until alone (forensic / audit) → retry ASC; the OLDEST entries are
//     most likely to fall before untilTS, whereas DESC would return the
//     latest N rows (all newer than untilTS) and silently produce empty output.
//   - --since (with or without --until) → retry DESC so the cap retains the
//     latest entries within the window; the user normally cares about recent
//     data filtered by --since.
//   - neither → retry DESC (latest rows by default).
//
// When untilSet is true and the fallback fires, an additional warning is
// emitted: --until cannot be enforced server-side without $filter, so
// historical entries past the retry window will be missed.
func fetchTxEntries(cl *client.Client, filter string, top int, orderDesc, sinceSet, untilSet bool) ([]model.TransactionLogEntry, bool, error) {
	endpoint := buildTxQuery(filter, top, orderDesc)
	data, err := cl.Get(endpoint)
	if err != nil {
		if filter != "" && isFilterRejection(err) {
			retryTop := fallbackRetryTop(top)
			retryDesc := true
			if untilSet && !sinceSet {
				retryDesc = false
			}
			retryData, retryErr := cl.Get(buildTxQuery("", retryTop, retryDesc))
			if retryErr != nil {
				return nil, false, retryErr
			}
			var resp model.TransactionLogResponse
			if jerr := json.Unmarshal(retryData, &resp); jerr != nil {
				return nil, false, fmt.Errorf("cannot parse server response: %w", jerr)
			}
			emitFallbackWarning(retryTop, len(resp.Value))
			if untilSet {
				output.PrintWarning("--until cannot be enforced server-side under fallback; historical entries past the retry window will be missed. Narrow --tail, set --since, or accept the limitation.")
			}
			warnIfPaginated(resp.NextLink)
			return resp.Value, true, nil
		}
		return nil, false, err
	}
	var resp model.TransactionLogResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, false, fmt.Errorf("cannot parse server response: %w", err)
	}
	warnIfPaginated(resp.NextLink)
	return resp.Value, false, nil
}

// applyTxClientFilters drops entries outside the [sinceTS, untilTS] window
// and any whose Cube/User don't match exactly (case-sensitive — matches
// server eq semantics).
func applyTxClientFilters(entries []model.TransactionLogEntry, sinceTS, untilTS, cube, user string) []model.TransactionLogEntry {
	var sinceT, untilT time.Time
	if sinceTS != "" {
		sinceT, _ = parseTimeStamp(sinceTS)
	}
	if untilTS != "" {
		untilT, _ = parseTimeStamp(untilTS)
	}

	out := make([]model.TransactionLogEntry, 0, len(entries))
	for _, e := range entries {
		if !sinceT.IsZero() || !untilT.IsZero() {
			t, err := parseTimeStamp(e.TimeStamp)
			if err != nil {
				continue
			}
			if !sinceT.IsZero() && t.Before(sinceT) {
				continue
			}
			if !untilT.IsZero() && t.After(untilT) {
				continue
			}
		}
		if cube != "" && e.Cube != cube {
			continue
		}
		if user != "" && e.User != user {
			continue
		}
		out = append(out, e)
	}
	return out
}

// sortTxByTimeStamp sorts ascending; ties are broken by ID. Unparseable
// timestamps go to the end stably.
func sortTxByTimeStamp(entries []model.TransactionLogEntry) {
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

// reverseTxEntries reverses entries in place.
func reverseTxEntries(entries []model.TransactionLogEntry) {
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
}

// boundaryTxIDs returns the maximum TimeStamp string in entries and the set
// of dedupe keys whose timestamp equals it. Used by --follow to drop
// boundary duplicates across polls.
func boundaryTxIDs(entries []model.TransactionLogEntry) (string, map[string]struct{}) {
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
		if e.TimeStamp != maxTS {
			continue
		}
		ids[txIDKey(e)] = struct{}{}
	}
	return maxTS, ids
}

// initialFollowWatermark composes boundaryTxIDs + advanceWatermark +
// resolveFollowWatermark to produce the starting (watermarkTS, watermarkIDs)
// pair for follow polls. txIDKey synthesizes content-based keys when TM1
// omits ID, so boundaryTxIDs returns a non-empty dedup set for any
// non-empty entries — making advanceWatermark a defensive no-op in that
// case. It still runs to handle the truly-empty (no entries) edge case.
func initialFollowWatermark(entries []model.TransactionLogEntry, sinceTS string, now time.Time) (string, map[string]struct{}) {
	maxTS, ids := boundaryTxIDs(entries)
	maxTS, ids = advanceWatermark(maxTS, ids)
	return resolveFollowWatermark(maxTS, sinceTS, now), ids
}

// printTxEntries renders entries in the chosen format. In follow+jsonMode it
// emits NDJSON (one object per line). In raw mode every string field is
// passed through sanitizeRawMessage so each entry stays on a single line.
func printTxEntries(entries []model.TransactionLogEntry, jsonMode, rawMode, isFollowChunk bool) {
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
			fmt.Printf("%s %s %s:%s %s -> %s %s\n",
				e.TimeStamp,
				sanitizeRawMessage(e.User),
				sanitizeRawMessage(e.Cube),
				sanitizeRawMessage(tupleString(e.Tuple)),
				sanitizeRawMessage(formatTxValue(e.OldValue)),
				sanitizeRawMessage(formatTxValue(e.NewValue)),
				sanitizeRawMessage(e.StatusMessage),
			)
		}
		return
	}
	headers := []string{"TIME", "USER", "CUBE", "TUPLE", "OLD", "NEW", "STATUS"}
	rows := make([][]string, len(entries))
	for i, e := range entries {
		rows[i] = []string{
			e.TimeStamp,
			sanitizeRawMessage(e.User),
			sanitizeRawMessage(e.Cube),
			sanitizeRawMessage(tupleString(e.Tuple)),
			sanitizeRawMessage(formatTxValue(e.OldValue)),
			sanitizeRawMessage(formatTxValue(e.NewValue)),
			sanitizeRawMessage(e.StatusMessage),
		}
	}
	output.PrintTable(headers, rows)
}

// followTxLogs is the polling loop driven by ctx for testability.
func followTxLogs(ctx context.Context, cl *client.Client, watermarkTS string, watermarkIDs map[string]struct{}, cube, user string, interval time.Duration, jsonMode, rawMode bool) error {
	if watermarkIDs == nil {
		watermarkIDs = map[string]struct{}{}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}

		filter := buildTxFilter(watermarkTS, "", cube, user)
		// --until is mutually exclusive with --follow, so untilSet is false.
		// sinceSet is true because the watermark itself is a since-anchor.
		entries, fallback, err := fetchTxEntries(cl, filter, 0, false, true, false)
		if err != nil {
			output.PrintWarning(fmt.Sprintf("poll failed: %s", err))
			continue
		}

		// Drop boundary duplicates first (clock-skew dedupe).
		filtered := entries[:0]
		for _, e := range entries {
			if _, seen := watermarkIDs[txIDKey(e)]; seen {
				continue
			}
			filtered = append(filtered, e)
		}
		entries = filtered

		// Compute next watermark from POST-DEDUP entries (what the server
		// actually surfaced this poll), not from POST-client-filter — otherwise
		// --cube/--user filtering everything out leaves the watermark frozen.
		seenMaxTS, seenIDs := boundaryTxIDs(entries)

		applySince, applyCube, applyUser := "", "", ""
		if fallback {
			applySince, applyCube, applyUser = watermarkTS, cube, user
		}
		if applySince != "" || applyCube != "" || applyUser != "" {
			entries = applyTxClientFilters(entries, applySince, "", applyCube, applyUser)
		}

		if len(entries) > 0 {
			sortTxByTimeStamp(entries)
			printTxEntries(entries, jsonMode, rawMode, true)
		}

		if seenMaxTS != "" {
			// When the new boundary timestamp matches the prior watermark,
			// MERGE the new IDs into the existing dedup set. Replacing would
			// drop prior boundary IDs and re-emit them on the next poll if
			// the server keeps returning them at the same TimeStamp.
			if seenMaxTS == watermarkTS {
				for k, v := range seenIDs {
					watermarkIDs[k] = v
				}
			} else {
				watermarkTS, watermarkIDs = advanceWatermark(seenMaxTS, seenIDs)
			}
		}
	}
}

func runLogsTx(cmd *cobra.Command, args []string) error {
	cfg, err := loadConfig()
	if err != nil {
		output.PrintError(err.Error(), isJSONOutput(nil))
		return errSilent
	}
	jsonMode := isJSONOutput(cfg)

	if logsTxRaw && jsonMode {
		output.PrintError("--raw cannot be combined with --output json.", jsonMode)
		return errSilent
	}
	if logsTxTail < 0 {
		output.PrintError("--tail must be non-negative.", jsonMode)
		return errSilent
	}
	if logsTxFollow && logsTxInterval <= 0 {
		output.PrintError("--interval must be greater than zero (e.g. 5s).", jsonMode)
		return errSilent
	}
	if logsTxFollow && logsTxUntil != "" {
		output.PrintError("--until cannot be combined with --follow.", jsonMode)
		return errSilent
	}

	now := time.Now()
	sinceTS, err := parseTimeFlag("--since", logsTxSince, now)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}
	untilTS, err := parseTimeFlag("--until", logsTxUntil, now)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}
	if sinceTS != "" && untilTS != "" {
		sinceT, _ := parseTimeStamp(sinceTS)
		untilT, _ := parseTimeStamp(untilTS)
		if untilT.Before(sinceT) {
			output.PrintError("--since must be earlier than --until.", jsonMode)
			return errSilent
		}
	}

	tail := defaultTailIfUnbounded(logsTxSince != "" || logsTxUntil != "", logsTxTail)

	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	filter := buildTxFilter(sinceTS, untilTS, logsTxCube, logsTxUser)
	entries, fallback, err := fetchTxEntries(cl, filter, tail, tail > 0, sinceTS != "", untilTS != "")
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	// On fallback, apply all four filters client-side. On the success path,
	// still apply --until client-side as defense in depth — TM1 may round
	// timestamps server-side and surface entries slightly outside the window.
	// We do NOT re-apply --since here: server-side `TimeStamp ge` is inclusive
	// and any rounding would only widen the lower bound by surfacing entries
	// at or near sinceTS, which is exactly what the user asked for.
	applySince, applyUntil, applyCube, applyUser := "", "", "", ""
	if fallback {
		applySince, applyUntil, applyCube, applyUser = sinceTS, untilTS, logsTxCube, logsTxUser
	} else if untilTS != "" {
		applyUntil = untilTS
	}
	if applySince != "" || applyUntil != "" || applyCube != "" || applyUser != "" {
		entries = applyTxClientFilters(entries, applySince, applyUntil, applyCube, applyUser)
	}

	// Fallback over-fetches via fallbackTailMultiplier so client-side filtering
	// has room to find matches; trim back to the user-requested --tail before
	// display. Server-returned entries are DESC, so [:tail] keeps the newest.
	if fallback && tail > 0 && len(entries) > tail {
		entries = entries[:tail]
	}

	if tail > 0 {
		reverseTxEntries(entries)
	} else {
		sortTxByTimeStamp(entries)
	}

	// In --follow + --output json, the initial batch is also NDJSON so the
	// stream is uniform — otherwise downstream parsers see a JSON array
	// followed by raw objects and reject the input as invalid.
	printTxEntries(entries, jsonMode, logsTxRaw, logsTxFollow)

	if !logsTxFollow {
		return nil
	}

	maxTS, ids := initialFollowWatermark(entries, sinceTS, time.Now())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	return followTxLogs(ctx, cl, maxTS, ids, logsTxCube, logsTxUser, logsTxInterval, jsonMode, logsTxRaw)
}

func init() {
	logsCmd.AddCommand(logsTxCmd)

	logsTxCmd.Flags().StringVar(&logsTxSince, "since", "", "Show entries newer than duration (e.g. 10m, 2h) or timestamp (e.g. 2026-04-24T10:00 — interpreted as local time when no timezone is given)")
	logsTxCmd.Flags().StringVar(&logsTxUntil, "until", "", "Show entries older than duration or timestamp (same syntax as --since)")
	logsTxCmd.Flags().StringVar(&logsTxCube, "cube", "", "Filter by cube name (server-side; case-sensitive)")
	logsTxCmd.Flags().StringVar(&logsTxUser, "user", "", "Filter by user name (server-side; case-sensitive)")
	logsTxCmd.Flags().BoolVarP(&logsTxFollow, "follow", "f", false, "Stream new entries kubectl-style (--output json emits NDJSON)")
	logsTxCmd.Flags().DurationVar(&logsTxInterval, "interval", 5*time.Second, "Polling interval when --follow is set")
	logsTxCmd.Flags().IntVar(&logsTxTail, "tail", 0, "Show last N entries (defaults to 100 when no --since/--follow)")
	logsTxCmd.Flags().BoolVar(&logsTxRaw, "raw", false, "Raw output: one line per entry (control characters in fields are collapsed to spaces)")
}
