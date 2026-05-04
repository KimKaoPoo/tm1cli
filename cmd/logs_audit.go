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
	logsAuditSince      string
	logsAuditUntil      string
	logsAuditObjectType string
	logsAuditObjectName string
	logsAuditUser       string
	logsAuditFollow     bool
	logsAuditInterval   time.Duration
	logsAuditTail       int
	logsAuditRaw        bool
)

var logsAuditCmd = &cobra.Command{
	Use:   "audit",
	Short: "Show and stream the TM1 audit log",
	Long: `Show and stream the TM1 audit log (object create/delete/update/rename events).

REST API: GET /AuditLogEntries

Prerequisite: the server must have audit logging enabled (AuditLog=T in
tm1s.cfg). When disabled, this command exits with a clear error.

Filter by time range, object type, object name, or user. Use --follow to
stream new entries kubectl-style; --tail to show the last N entries.

--object-type, --object-name, and --user are matched case-sensitively
(server-side OData eq); when the server cannot apply $filter, the client
falls back to the same case-sensitive equality.

When --follow is combined with --output json, entries are emitted as
NDJSON (one JSON object per line) instead of a JSON array.

Note: an empty result set may also indicate insufficient permission to read
the audit log on this server.`,
	Example: `  tm1cli logs audit --tail 50
  tm1cli logs audit --since 24h --object-type Cube
  tm1cli logs audit --user admin --follow
  tm1cli logs audit --since 2026-04-24T10:00 --until 2026-04-24T18:00 --raw
  tm1cli logs audit --object-type Process --object-name ImportSales`,
	RunE: runLogsAudit,
}

var errAuditLogDisabled = errors.New("Audit log is disabled on this TM1 server. Set AuditLog=T in tm1s.cfg and restart the server.")

// isAuditLogDisabled returns true when the server error indicates audit
// logging is not enabled. The signal varies by TM1 version, so accept
// any of "disabled" / "not enabled" / "not supported" / "not available"
// in combination with the feature phrase ("audit log" or "auditing").
// The bare token "audit" is rejected because it appears inside the
// entity-set name AuditLogEntries — server bodies that echo the entity
// name in a $filter-rejection (e.g. "$filter is not supported for
// AuditLogEntries") would otherwise be misclassified as disabled and
// suppress the required client-side fallback. Auth and not-found errors
// are also excluded: they imply nothing about audit-log config.
func isAuditLogDisabled(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if strings.HasPrefix(msg, "HTTP 401") || strings.HasPrefix(msg, "HTTP 403") || strings.HasPrefix(msg, "HTTP 404") {
		return false
	}
	if strings.Contains(msg, "Authentication failed") {
		return false
	}
	lower := strings.ToLower(msg)
	// Require the feature phrase to avoid matching "AuditLogEntries"
	// (entity-set name) which lowercases to "auditlogentries" — neither
	// "audit log" (note the space) nor "auditing" appears in it.
	if !strings.Contains(lower, "audit log") && !strings.Contains(lower, "auditing") {
		return false
	}
	return strings.Contains(lower, "disabled") ||
		strings.Contains(lower, "not enabled") ||
		strings.Contains(lower, "not supported") ||
		strings.Contains(lower, "not available")
}

// buildAuditFilter assembles the OData $filter from time bounds and exact-match
// objectType/objectName/user. Empty inputs are skipped. All string values use
// OData eq with odata-escaped string literals.
func buildAuditFilter(sinceTS, untilTS, objectType, objectName, user string) string {
	var parts []string
	if sinceTS != "" {
		parts = append(parts, fmt.Sprintf("TimeStamp ge %s", sinceTS))
	}
	if untilTS != "" {
		parts = append(parts, fmt.Sprintf("TimeStamp le %s", untilTS))
	}
	if objectType != "" {
		parts = append(parts, fmt.Sprintf("ObjectType eq '%s'", odataEscape(objectType)))
	}
	if objectName != "" {
		parts = append(parts, fmt.Sprintf("ObjectName eq '%s'", odataEscape(objectName)))
	}
	if user != "" {
		parts = append(parts, fmt.Sprintf("User eq '%s'", odataEscape(user)))
	}
	return strings.Join(parts, " and ")
}

// buildAuditQuery builds the endpoint URL with $filter, $top, $orderby using
// url.Values for safe encoding. orderBy must be "", orderTimeStampAsc, or
// orderTimeStampDesc — empty means no $orderby clause (use server default).
func buildAuditQuery(filter string, top int, orderBy string) string {
	v := url.Values{}
	if filter != "" {
		v.Set("$filter", filter)
	}
	if top > 0 {
		v.Set("$top", strconv.Itoa(top))
	}
	if orderBy != "" {
		v.Set("$orderby", orderBy)
	}
	if len(v) == 0 {
		return "AuditLogEntries"
	}
	return "AuditLogEntries?" + v.Encode()
}

// fetchAuditEntries performs GET. On HTTP 400/501 with a filter-rejection body,
// it retries without $filter (over-fetching via fallbackRetryTop when --tail is
// set so client-side filtering has room to find matches) and returns fallback=true.
// On disabled-audit errors the friendly errAuditLogDisabled is returned.
// On auth/not-found/network errors the error propagates unchanged.
//
// sinceSet/untilSet steer the retry order so the cap captures the right slice
// of the log:
//   - --until alone → retry ASC so the cap captures the oldest entries (most
//     likely before untilTS), avoiding silent empty output.
//   - --since (with or without --until) → retry DESC for the latest entries.
//   - neither → retry DESC (latest rows by default).
func fetchAuditEntries(cl *client.Client, filter string, top int, orderDesc, sinceSet, untilSet bool) ([]model.AuditLogEntry, bool, error) {
	successOrderBy := ""
	if orderDesc {
		successOrderBy = orderTimeStampDesc
	}
	endpoint := buildAuditQuery(filter, top, successOrderBy)
	data, err := cl.Get(endpoint)
	if err != nil {
		if isAuditLogDisabled(err) {
			return nil, false, errAuditLogDisabled
		}
		if filter != "" && isFilterRejection(err) {
			retryTop := fallbackRetryTop(top)
			retryOrderBy := orderTimeStampDesc
			if untilSet && !sinceSet {
				retryOrderBy = orderTimeStampAsc
			}
			retryData, retryErr := cl.Get(buildAuditQuery("", retryTop, retryOrderBy))
			if retryErr != nil {
				if isAuditLogDisabled(retryErr) {
					return nil, false, errAuditLogDisabled
				}
				return nil, false, retryErr
			}
			var resp model.AuditLogResponse
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
	var resp model.AuditLogResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, false, fmt.Errorf("cannot parse server response: %w", err)
	}
	warnIfPaginated(resp.NextLink)
	return resp.Value, false, nil
}

// applyAuditClientFilters drops entries outside the [sinceTS, untilTS] window
// and any whose ObjectType/ObjectName/User don't match exactly (case-sensitive —
// matches server eq semantics).
func applyAuditClientFilters(entries []model.AuditLogEntry, sinceTS, untilTS, objectType, objectName, user string) []model.AuditLogEntry {
	var sinceT, untilT time.Time
	if sinceTS != "" {
		sinceT, _ = parseTimeStamp(sinceTS)
	}
	if untilTS != "" {
		untilT, _ = parseTimeStamp(untilTS)
	}

	out := make([]model.AuditLogEntry, 0, len(entries))
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
		if objectType != "" && e.ObjectType != objectType {
			continue
		}
		if objectName != "" && e.ObjectName != objectName {
			continue
		}
		if user != "" && e.User != user {
			continue
		}
		out = append(out, e)
	}
	return out
}

// sortAuditByTimeStamp sorts ascending; ties are broken by ID (string compare).
// Unparseable timestamps go to the end stably.
func sortAuditByTimeStamp(entries []model.AuditLogEntry) {
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

// sortAuditByTimeStampDesc sorts descending; ties are broken by ID (descending).
// Unparseable timestamps go to the end stably.
func sortAuditByTimeStampDesc(entries []model.AuditLogEntry) {
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
			return entries[i].ID > entries[j].ID
		default:
			return ti.After(tj)
		}
	})
}

// reverseAuditEntries reverses entries in place.
func reverseAuditEntries(entries []model.AuditLogEntry) {
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
}

// auditIDKey returns the dedupe key for an audit-log entry. When TM1 supplies
// ID we use it directly. When ID is omitted (older versions) we synthesize a
// key from the observable fields so same-timestamp entries can be distinguished.
// Two functionally identical events (same content, same timestamp) hash to the
// same key, which is the right behavior — they are indistinguishable.
func auditIDKey(e model.AuditLogEntry) string {
	if e.ID != "" {
		return e.ID
	}
	// 0x1f (unit separator) is illegal in TM1 names and OData strings, so
	// it cannot appear inside any field — safe as a delimiter.
	const sep = "\x1f"
	return strings.Join([]string{
		"syn",
		e.TimeStamp,
		e.User,
		e.ObjectType,
		e.ObjectName,
		e.Description,
		e.AuditDetails,
	}, sep)
}

// boundaryAuditIDs returns the maximum TimeStamp string in entries and the set
// of dedupe keys whose timestamp equals it. Used by --follow to drop boundary
// duplicates across polls.
func boundaryAuditIDs(entries []model.AuditLogEntry) (string, map[string]struct{}) {
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
		ids[auditIDKey(e)] = struct{}{}
	}
	return maxTS, ids
}

// initialFollowAuditWatermark composes boundaryAuditIDs + advanceWatermark +
// resolveFollowWatermark to produce the starting (watermarkTS, watermarkIDs)
// pair for follow polls.
func initialFollowAuditWatermark(entries []model.AuditLogEntry, sinceTS string, now time.Time) (string, map[string]struct{}) {
	maxTS, ids := boundaryAuditIDs(entries)
	maxTS, ids = advanceWatermark(maxTS, ids)
	return resolveFollowWatermark(maxTS, sinceTS, now), ids
}

// printAuditEntries renders entries in the chosen format. In follow+jsonMode it
// emits NDJSON (one object per line). In raw mode every string field is passed
// through sanitizeRawMessage so each entry stays on a single line.
func printAuditEntries(entries []model.AuditLogEntry, jsonMode, rawMode, isFollowChunk bool) {
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
			fmt.Printf("%s %s %s %s %s\n",
				e.TimeStamp,
				sanitizeRawMessage(e.User),
				sanitizeRawMessage(e.ObjectType),
				sanitizeRawMessage(e.ObjectName),
				sanitizeRawMessage(e.Description),
			)
		}
		return
	}
	headers := []string{"TIME", "USER", "OBJECT_TYPE", "OBJECT_NAME", "DESCRIPTION"}
	rows := make([][]string, len(entries))
	for i, e := range entries {
		rows[i] = []string{
			e.TimeStamp,
			sanitizeRawMessage(e.User),
			sanitizeRawMessage(e.ObjectType),
			sanitizeRawMessage(e.ObjectName),
			sanitizeRawMessage(e.Description),
		}
	}
	output.PrintTable(headers, rows)
}

// followAuditLogs is the polling loop driven by ctx for testability.
func followAuditLogs(ctx context.Context, cl *client.Client, watermarkTS string, watermarkIDs map[string]struct{}, objectType, objectName, user string, interval time.Duration, jsonMode, rawMode bool) error {
	if watermarkIDs == nil {
		watermarkIDs = map[string]struct{}{}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}

		filter := buildAuditFilter(watermarkTS, "", objectType, objectName, user)
		// --until is mutually exclusive with --follow, so untilSet is false.
		// sinceSet is true because the watermark itself is a since-anchor.
		entries, fallback, err := fetchAuditEntries(cl, filter, 0, false, true, false)
		if err != nil {
			output.PrintWarning(fmt.Sprintf("poll failed: %s", err))
			continue
		}

		// Drop boundary duplicates first (clock-skew dedupe).
		filtered := entries[:0]
		for _, e := range entries {
			if _, seen := watermarkIDs[auditIDKey(e)]; seen {
				continue
			}
			filtered = append(filtered, e)
		}
		entries = filtered

		// Compute next watermark from POST-DEDUP entries (what the server
		// actually surfaced this poll), not from POST-client-filter — otherwise
		// objectType/objectName/user filtering everything out leaves the watermark frozen.
		seenMaxTS, seenIDs := boundaryAuditIDs(entries)

		applySince, applyObjectType, applyObjectName, applyUser := "", "", "", ""
		if fallback {
			applySince, applyObjectType, applyObjectName, applyUser = watermarkTS, objectType, objectName, user
		}
		if applySince != "" || applyObjectType != "" || applyObjectName != "" || applyUser != "" {
			entries = applyAuditClientFilters(entries, applySince, "", applyObjectType, applyObjectName, applyUser)
		}

		if len(entries) > 0 {
			sortAuditByTimeStamp(entries)
			printAuditEntries(entries, jsonMode, rawMode, true)
		}

		if seenMaxTS != "" {
			// The watermark must be MONOTONIC — never regress. Under fallback,
			// the server returns the unfiltered log; dedupe can strip the
			// latest rows leaving older entries whose seenMaxTS is BEFORE
			// watermarkTS. Accepting that would forget the prior boundary IDs
			// and re-emit the original entries on the next poll, indefinitely.
			seenT, seenErr := parseTimeStamp(seenMaxTS)
			curT, curErr := parseTimeStamp(watermarkTS)
			if seenMaxTS == watermarkTS {
				// Same boundary: MERGE new IDs into the existing dedup set.
				// Replacing would drop prior boundary IDs and re-emit them
				// next poll if the server keeps returning them at this TS.
				for k, v := range seenIDs {
					watermarkIDs[k] = v
				}
			} else if seenErr == nil && (curErr != nil || seenT.After(curT)) {
				// Strictly later boundary: advance.
				watermarkTS, watermarkIDs = advanceWatermark(seenMaxTS, seenIDs)
			}
			// else: seenMaxTS is older than watermarkTS — ignore (do not regress).
		}
	}
}

func runLogsAudit(cmd *cobra.Command, args []string) error {
	cfg, err := loadConfig()
	if err != nil {
		output.PrintError(err.Error(), isJSONOutput(nil))
		return errSilent
	}
	jsonMode := isJSONOutput(cfg)

	if logsAuditRaw && jsonMode {
		output.PrintError("--raw cannot be combined with --output json.", jsonMode)
		return errSilent
	}
	if logsAuditTail < 0 {
		output.PrintError("--tail must be non-negative.", jsonMode)
		return errSilent
	}
	if logsAuditFollow && logsAuditInterval <= 0 {
		output.PrintError("--interval must be greater than zero (e.g. 5s).", jsonMode)
		return errSilent
	}
	if logsAuditFollow && logsAuditUntil != "" {
		output.PrintError("--until cannot be combined with --follow.", jsonMode)
		return errSilent
	}

	now := time.Now()
	sinceTS, err := parseTimeFlag("--since", logsAuditSince, now)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}
	untilTS, err := parseTimeFlag("--until", logsAuditUntil, now)
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

	tail := defaultTailIfUnbounded(logsAuditSince != "" || logsAuditUntil != "", logsAuditTail)

	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	filter := buildAuditFilter(sinceTS, untilTS, logsAuditObjectType, logsAuditObjectName, logsAuditUser)
	entries, fallback, err := fetchAuditEntries(cl, filter, tail, tail > 0, sinceTS != "", untilTS != "")
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	// On fallback, apply all five filters client-side. On the success path,
	// still apply --until client-side as defense in depth — TM1 may round
	// timestamps server-side and surface entries slightly outside the window.
	applySince, applyUntil, applyObjectType, applyObjectName, applyUser := "", "", "", "", ""
	if fallback {
		applySince, applyUntil, applyObjectType, applyObjectName, applyUser = sinceTS, untilTS, logsAuditObjectType, logsAuditObjectName, logsAuditUser
	} else if untilTS != "" {
		applyUntil = untilTS
	}
	if applySince != "" || applyUntil != "" || applyObjectType != "" || applyObjectName != "" || applyUser != "" {
		entries = applyAuditClientFilters(entries, applySince, applyUntil, applyObjectType, applyObjectName, applyUser)
	}

	// Fallback over-fetches via fallbackTailMultiplier so client-side filtering
	// has room to find matches; trim back to the user-requested --tail before
	// display. Sort DESC first so [:tail] always keeps the LATEST N.
	if fallback && tail > 0 && len(entries) > tail {
		sortAuditByTimeStampDesc(entries)
		entries = entries[:tail]
	}

	if tail > 0 {
		reverseAuditEntries(entries)
	} else {
		sortAuditByTimeStamp(entries)
	}

	// In --follow + --output json, the initial batch is also NDJSON so the
	// stream is uniform.
	printAuditEntries(entries, jsonMode, logsAuditRaw, logsAuditFollow)

	if !logsAuditFollow {
		return nil
	}

	maxTS, ids := initialFollowAuditWatermark(entries, sinceTS, time.Now())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	return followAuditLogs(ctx, cl, maxTS, ids, logsAuditObjectType, logsAuditObjectName, logsAuditUser, logsAuditInterval, jsonMode, logsAuditRaw)
}

func init() {
	logsCmd.AddCommand(logsAuditCmd)

	logsAuditCmd.Flags().StringVar(&logsAuditSince, "since", "", "Show entries newer than duration (e.g. 10m, 2h) or timestamp (e.g. 2026-04-24T10:00 — interpreted as local time when no timezone is given)")
	logsAuditCmd.Flags().StringVar(&logsAuditUntil, "until", "", "Show entries older than duration or timestamp (same syntax as --since)")
	logsAuditCmd.Flags().StringVar(&logsAuditObjectType, "object-type", "", "Filter by object type (server-side; case-sensitive, e.g. Cube, Process, Dimension)")
	logsAuditCmd.Flags().StringVar(&logsAuditObjectName, "object-name", "", "Filter by object name (server-side; case-sensitive)")
	logsAuditCmd.Flags().StringVar(&logsAuditUser, "user", "", "Filter by user name (server-side; case-sensitive)")
	logsAuditCmd.Flags().BoolVarP(&logsAuditFollow, "follow", "f", false, "Stream new entries kubectl-style (--output json emits NDJSON)")
	logsAuditCmd.Flags().DurationVar(&logsAuditInterval, "interval", 5*time.Second, "Polling interval when --follow is set")
	logsAuditCmd.Flags().IntVar(&logsAuditTail, "tail", 0, "Show last N entries (defaults to 100 when no --since/--follow)")
	logsAuditCmd.Flags().BoolVar(&logsAuditRaw, "raw", false, "Raw output: one line per entry (control characters in fields are collapsed to spaces)")
}
