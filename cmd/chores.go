package cmd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
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
	choresFilter           string
	choresActive           bool
	choresInactive         bool
	choresLimit            int
	choresAll              bool
	choresShowSystem       bool
	choresActivateYes      bool
	choresActivateDryRun   bool
	choresDeactivateYes    bool
	choresDeactivateDryRun bool
)

const defaultChoreTimeout = 30 * time.Minute

var (
	choreRunAsync   bool
	choreRunTimeout time.Duration
)

var choresCmd = &cobra.Command{
	Use:   "chores",
	Short: "View TM1 chores and schedules",
	Long:  `View TM1 chores (scheduled task chains) and their step-by-step task lists.`,
}

var choresListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all chores",
	// Suppress cobra's auto-appended usage block on RunE errors so stderr in
	// --output json mode stays a single, parseable JSON object.
	SilenceUsage: true,
	Long: `List all chores on the TM1 server.

REST API: GET /Chores

System chores (names starting with }) are hidden by default. Results are
limited to 50 by default; use --all to show everything.

Frequency is rendered human-readable in table mode (e.g. "Every 1 day"); JSON
mode preserves the raw ISO 8601 duration so it remains machine-readable.`,
	Example: `  tm1cli chores list
  tm1cli chores list --active
  tm1cli chores list --filter "load"
  tm1cli chores list --all
  tm1cli chores list --show-system
  tm1cli chores list --output json`,
	RunE: runChoresList,
}

var choresShowCmd = &cobra.Command{
	Use:   "show <name>",
	Short: "Show a chore's task list",
	// Suppress cobra's auto-appended usage block on RunE errors so stderr in
	// --output json mode stays a single, parseable JSON object.
	SilenceUsage: true,
	Long: `Show the step-by-step task list for a chore: each step's process and parameters.

REST API: GET /Chores('name')?$expand=Tasks($expand=Process,Parameters)

Tasks are sorted by step number ascending. JSON mode returns the raw chore object.

Exit codes:
  0  chore found and displayed
  1  generic error (auth, network, server)
  3  chore not found`,
	Example: `  tm1cli chores show "DailyLoad"
  tm1cli chores show "DailyLoad" --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runChoresShow,
}

// odataKey escapes a TM1 entity name for OData URL keys: doubles single
// quotes per the OData literal spec, then URL-path-escapes for transport.
func odataKey(name string) string {
	return url.PathEscape(odataEscape(name))
}

// formatChoreFrequency converts ISO 8601 duration strings (e.g. "P1D",
// "P1DT0H0M0S", "PT30M") into human-readable phrases. Returns the original
// string on any parse failure (e.g. "P1W", "P1Y", "P1DT") so callers always
// see meaningful output.
func formatChoreFrequency(s string) string {
	days, hours, minutes, seconds, ok := parseISO8601Duration(s)
	if !ok {
		return s
	}
	var parts []string
	if days != 0 {
		parts = append(parts, plural(days, "day"))
	}
	if hours != 0 {
		parts = append(parts, plural(hours, "hour"))
	}
	if minutes != 0 {
		parts = append(parts, plural(minutes, "minute"))
	}
	if seconds != 0 {
		parts = append(parts, plural(seconds, "second"))
	}
	if len(parts) == 0 {
		return "Every 0 seconds"
	}
	return "Every " + strings.Join(parts, " ")
}

// parseISO8601Duration parses the subset P[nD][T[nH][nM][nS]] used by TM1
// chore frequencies. Returns ok=false for unsupported designators (Y, W,
// month-M before T) or malformed input.
func parseISO8601Duration(s string) (days, hours, minutes, seconds int, ok bool) {
	if len(s) < 2 || s[0] != 'P' {
		return 0, 0, 0, 0, false
	}
	rest := s[1:]
	var datePart, timePart string
	if i := strings.Index(rest, "T"); i >= 0 {
		datePart = rest[:i]
		timePart = rest[i+1:]
		if timePart == "" {
			return 0, 0, 0, 0, false
		}
	} else {
		datePart = rest
	}
	if datePart != "" {
		n, designator, remaining, parseOk := nextDurationField(datePart)
		if !parseOk || designator != 'D' || remaining != "" {
			return 0, 0, 0, 0, false
		}
		days = n
	}
	for timePart != "" {
		n, designator, remaining, parseOk := nextDurationField(timePart)
		if !parseOk {
			return 0, 0, 0, 0, false
		}
		switch designator {
		case 'H':
			if hours != 0 || minutes != 0 || seconds != 0 {
				return 0, 0, 0, 0, false
			}
			hours = n
		case 'M':
			if minutes != 0 || seconds != 0 {
				return 0, 0, 0, 0, false
			}
			minutes = n
		case 'S':
			if seconds != 0 {
				return 0, 0, 0, 0, false
			}
			seconds = n
		default:
			return 0, 0, 0, 0, false
		}
		timePart = remaining
	}
	return days, hours, minutes, seconds, true
}

// nextDurationField scans leading digits then a single uppercase designator.
// Returns ok=false if the digit run is empty or the designator is missing.
func nextDurationField(s string) (n int, designator byte, rest string, ok bool) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 || i == len(s) {
		return 0, 0, "", false
	}
	parsed, err := strconv.Atoi(s[:i])
	if err != nil {
		return 0, 0, "", false
	}
	return parsed, s[i], s[i+1:], true
}

func plural(n int, unit string) string {
	if n == 1 {
		return "1 " + unit
	}
	return fmt.Sprintf("%d %ss", n, unit)
}

func runChoresList(cmd *cobra.Command, args []string) error {
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

	// --active and --inactive are mutually exclusive. We enforce this at
	// runtime instead of cobra.MarkFlagsMutuallyExclusive because pflag.Changed
	// leaks across rootCmd.Execute() calls in the test harness, which makes
	// the cobra-side check trip on flags that were "set" in a prior test.
	if choresActive && choresInactive {
		output.PrintError("--active and --inactive are mutually exclusive.", jsonMode)
		return errSilent
	}

	showSystem := getShowSystem(cfg, choresShowSystem)
	limit := getLimit(cfg, choresLimit, choresAll)

	const base = "Chores?$select=Name,Active,StartTime,DSTSensitive,Frequency&$expand=Tasks($select=Step)"

	// Match cubes/dims: only --filter forces a full fetch (server-side
	// $filter may not be honored, requiring client-side substring matching
	// over the complete set). The +500 cushion absorbs --show-system /
	// --active / --inactive client-side trimming so they don't silently lose
	// matches at the truncation boundary.
	fetchEndpoint := base
	if limit > 0 && choresFilter == "" {
		fetchEndpoint = fmt.Sprintf("%s&$top=%d", base, limit+500)
	}

	chores, err := fetchChores(cl, fetchEndpoint, choresFilter)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	chores = filterSystemChores(chores, showSystem)
	chores = filterChoresByActive(chores, choresActive, choresInactive)
	displayChores(chores, limit, jsonMode)
	return nil
}

func filterSystemChores(chores []model.Chore, showSystem bool) []model.Chore {
	if showSystem {
		return chores
	}
	var out []model.Chore
	for _, c := range chores {
		if !strings.HasPrefix(c.Name, "}") {
			out = append(out, c)
		}
	}
	return out
}

// fetchChores tries server-side $filter first, falling back to client-side
// filtering with a [warn] when the server rejects the filter. Returns the raw
// fetch error so the caller controls how it's reported.
func fetchChores(cl *client.Client, base, filter string) ([]model.Chore, error) {
	if filter == "" {
		return getChores(cl, base)
	}

	v := url.Values{}
	v.Set("$filter", fmt.Sprintf("contains(tolower(Name),tolower('%s'))", odataEscape(filter)))
	if chores, err := getChores(cl, base+"&"+v.Encode()); err == nil {
		return chores, nil
	}

	output.PrintWarning("Server-side filter not supported, filtering locally...")
	chores, err := getChores(cl, base)
	if err != nil {
		return nil, err
	}
	return filterChoresByName(chores, filter), nil
}

func getChores(cl *client.Client, endpoint string) ([]model.Chore, error) {
	data, err := cl.Get(endpoint)
	if err != nil {
		return nil, err
	}
	var resp model.ChoreResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("Cannot parse server response.")
	}
	return resp.Value, nil
}

func filterChoresByName(chores []model.Chore, filter string) []model.Chore {
	lower := strings.ToLower(filter)
	var out []model.Chore
	for _, c := range chores {
		if strings.Contains(strings.ToLower(c.Name), lower) {
			out = append(out, c)
		}
	}
	return out
}

func filterChoresByActive(chores []model.Chore, active, inactive bool) []model.Chore {
	if !active && !inactive {
		return chores
	}
	var out []model.Chore
	for _, c := range chores {
		if active && c.Active {
			out = append(out, c)
		} else if inactive && !c.Active {
			out = append(out, c)
		}
	}
	return out
}

func displayChores(chores []model.Chore, limit int, jsonMode bool) {
	total := len(chores)
	shown := chores
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}

	if jsonMode {
		if shown == nil {
			shown = []model.Chore{}
		}
		output.PrintJSON(shown)
		return
	}
	headers := []string{"NAME", "ACTIVE", "STARTTIME", "DSTSENSITIVE", "FREQUENCY", "TASKS"}
	rows := make([][]string, len(shown))
	for i, c := range shown {
		rows[i] = []string{
			c.Name,
			strconv.FormatBool(c.Active),
			c.StartTime,
			strconv.FormatBool(c.DSTSensitive),
			formatChoreFrequency(c.Frequency),
			strconv.Itoa(len(c.Tasks)),
		}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(shown), total)
}

func runChoresShow(cmd *cobra.Command, args []string) error {
	name := args[0]

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

	endpoint := fmt.Sprintf("Chores('%s')?$expand=Tasks($expand=Process($select=Name),Parameters)", odataKey(name))
	data, err := cl.Get(endpoint)
	if err != nil {
		if errors.Is(err, client.ErrNotFound) {
			output.PrintError(fmt.Sprintf("Chore '%s' not found.", name), jsonMode)
			return errExit(3)
		}
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var chore model.Chore
	if err := json.Unmarshal(data, &chore); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	sort.SliceStable(chore.Tasks, func(i, j int) bool {
		return chore.Tasks[i].Step < chore.Tasks[j].Step
	})

	if jsonMode {
		output.PrintJSON(chore)
		return nil
	}

	fmt.Printf("Name:           %s\n", chore.Name)
	fmt.Printf("Active:         %s\n", strconv.FormatBool(chore.Active))
	fmt.Printf("StartTime:      %s\n", chore.StartTime)
	fmt.Printf("DSTSensitive:   %s\n", strconv.FormatBool(chore.DSTSensitive))
	fmt.Printf("Frequency:      %s\n", formatChoreFrequency(chore.Frequency))
	fmt.Println()

	headers := []string{"STEP", "PROCESS", "PARAMETERS"}
	rows := make([][]string, len(chore.Tasks))
	for i, t := range chore.Tasks {
		rows[i] = []string{
			strconv.Itoa(t.Step),
			t.Process.Name,
			renderChoreParams(t.Parameters),
		}
	}
	output.PrintTable(headers, rows)
	return nil
}

func renderChoreParams(params []model.ChoreTaskParam) string {
	if len(params) == 0 {
		return "(none)"
	}
	parts := make([]string, len(params))
	for i, p := range params {
		parts[i] = fmt.Sprintf("%s=%v", p.Name, p.Value)
	}
	return strings.Join(parts, ", ")
}

var choresActivateCmd = &cobra.Command{
	Use:          "activate <name>",
	Short:        "Activate a chore (resume its schedule)",
	SilenceUsage: true,
	Long: `Activate a chore so the TM1 server resumes running it on schedule.

REST API: POST /Chores('name')/tm1.Activate

If the chore is already active, the command prints an info message and exits 0
without contacting the server again (idempotent).

The command prompts for confirmation by default. Use --yes to skip the prompt
for scripting. Use --dry-run to preview without sending the POST; --dry-run
takes precedence over --yes.

Exit codes:
  0  chore activated (or already active)
  1  generic error (auth, network, server, permission denied)
  3  chore not found`,
	Example: `  tm1cli chores activate "DailyLoad"
  tm1cli chores activate "DailyLoad" --yes
  tm1cli chores activate "DailyLoad" --dry-run
  tm1cli chores activate "DailyLoad" --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runChoresActivate,
}

var choresDeactivateCmd = &cobra.Command{
	Use:          "deactivate <name>",
	Short:        "Deactivate a chore (suspend its schedule)",
	SilenceUsage: true,
	Long: `Deactivate a chore so the TM1 server stops running it on schedule.

REST API: POST /Chores('name')/tm1.Deactivate

If the chore is already inactive, the command prints an info message and exits 0
without contacting the server again (idempotent).

The command prompts for confirmation by default. Use --yes to skip the prompt
for scripting. Use --dry-run to preview without sending the POST; --dry-run
takes precedence over --yes.

Exit codes:
  0  chore deactivated (or already inactive)
  1  generic error (auth, network, server, permission denied)
  3  chore not found`,
	Example: `  tm1cli chores deactivate "DailyLoad"
  tm1cli chores deactivate "DailyLoad" --yes
  tm1cli chores deactivate "DailyLoad" --dry-run
  tm1cli chores deactivate "DailyLoad" --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runChoresDeactivate,
}

func runChoresActivate(cmd *cobra.Command, args []string) error {
	return runChoresToggle(args[0], true, choresActivateYes, choresActivateDryRun)
}

func runChoresDeactivate(cmd *cobra.Command, args []string) error {
	return runChoresToggle(args[0], false, choresDeactivateYes, choresDeactivateDryRun)
}

// choreEndpoint formats a Chores-collection endpoint with the chore name
// OData- and URL-escaped. Mirrors threadEndpoint: empty suffix yields the
// bare entity URL, otherwise the suffix is joined as an action segment.
func choreEndpoint(name, suffix string) string {
	if suffix == "" {
		return fmt.Sprintf("Chores('%s')", odataKey(name))
	}
	return fmt.Sprintf("Chores('%s')/%s", odataKey(name), suffix)
}

// runChoresToggle drives `chores activate` and `chores deactivate`. target
// is the desired Active state. The flow is:
//  1. GET to verify existence and read current Active state.
//  2. If already in target state, emit info and return success (idempotent).
//  3. If dry-run, print preview and return.
//  4. Else, prompt unless yes==true, then POST tm1.Activate / tm1.Deactivate.
func runChoresToggle(name string, target, yes, dryRun bool) error {
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

	labels := choreToggleLabels(target)

	chore, err := fetchChoreActive(cl, name)
	if err != nil {
		return handleChoreToggleError(err, name, jsonMode)
	}

	if chore.Active == target {
		if jsonMode {
			output.PrintJSON(map[string]string{
				"status":  "noop",
				"chore":   name,
				"active":  strconv.FormatBool(chore.Active),
				"message": fmt.Sprintf("Chore '%s' is already %s.", name, labels.state),
			})
		} else {
			fmt.Printf("Chore '%s' is already %s. No change.\n", name, labels.state)
		}
		return nil
	}

	if dryRun {
		if jsonMode {
			output.PrintJSON(map[string]string{
				"status": "dry-run",
				"chore":  name,
				"action": labels.verb,
			})
		} else {
			fmt.Printf("[dry-run] Would %s chore '%s'.\n", labels.verb, name)
		}
		return nil
	}

	if !yes {
		fmt.Fprintf(os.Stderr, "About to %s chore '%s'.\n", labels.verb, name)
		if !promptYesNo(bufio.NewReader(os.Stdin), "Continue?") {
			return nil
		}
	}

	if _, err := cl.Post(choreEndpoint(name, labels.op), map[string]interface{}{}); err != nil {
		return handleChoreToggleError(err, name, jsonMode)
	}

	if jsonMode {
		output.PrintJSON(map[string]string{
			"status": labels.past,
			"chore":  name,
		})
	} else {
		fmt.Printf("Chore '%s' %s.\n", name, labels.past)
	}
	return nil
}

// choreToggleLabels returns the action vocabulary for a toggle direction:
// the verb ("activate"), the OData operation segment ("tm1.Activate"), the
// past-tense status ("activated"), and the post-toggle state name ("active").
func choreToggleLabels(target bool) struct{ verb, op, past, state string } {
	if target {
		return struct{ verb, op, past, state string }{"activate", "tm1.Activate", "activated", "active"}
	}
	return struct{ verb, op, past, state string }{"deactivate", "tm1.Deactivate", "deactivated", "inactive"}
}

// fetchChoreActive issues GET Chores('name')?$select=Name,Active and returns
// a populated Chore. It probes Active via *bool so a missing field surfaces
// as an explicit error rather than silently defaulting to false.
func fetchChoreActive(cl *client.Client, name string) (*model.Chore, error) {
	data, err := cl.Get(choreEndpoint(name, "") + "?$select=Name,Active")
	if err != nil {
		return nil, err
	}
	var probe struct {
		Name   string `json:"Name"`
		Active *bool  `json:"Active"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, fmt.Errorf("Cannot parse server response.")
	}
	if probe.Active == nil {
		return nil, fmt.Errorf("Server response missing 'Active' field for chore '%s'.", name)
	}
	return &model.Chore{Name: probe.Name, Active: *probe.Active}, nil
}

// handleChoreToggleError funnels common failure modes through one place:
// not-found maps to exit code 3, 403 to a friendlier permission message.
// Falls through to the raw client error otherwise. Mirrors handleThreadCancelError;
// if internal/client later exposes typed status errors, swap both at once.
func handleChoreToggleError(err error, name string, jsonMode bool) error {
	if errors.Is(err, client.ErrNotFound) {
		output.PrintError(fmt.Sprintf("Chore '%s' not found.", name), jsonMode)
		return errExit(3)
	}
	if strings.HasPrefix(err.Error(), "HTTP 403") {
		output.PrintError("Permission denied. Activating or deactivating chores requires admin privileges.", jsonMode)
		return errSilent
	}
	output.PrintError(err.Error(), jsonMode)
	return errSilent
}

var choresRunCmd = &cobra.Command{
	Use:          "run <name>",
	Short:        "Execute a chore immediately",
	SilenceUsage: true,
	Long: `Execute a chore on the TM1 server.

REST API: POST /Chores('name')/tm1.Execute

By default, the command waits for the chore to complete (up to --timeout).
Use --async to return immediately with the server-side thread ID.

With --verbose, the chore's task list is printed to stderr before the run.
TM1's tm1.Execute does not stream per-task results, so step-level failure
detail comes from the server's error message on failure (typically HTTP
5xx) rather than from a structured per-task status.

Exit codes:
  0  chore executed successfully (sync) or async accepted
  1  generic error (auth, network, server, task failure)
  3  chore not found
  4  wait exceeded --timeout`,
	Example: `  tm1cli chores run "Nightly Load"
  tm1cli chores run "Nightly Load" --async
  tm1cli chores run "Nightly Load" --timeout 1h
  tm1cli chores run "Nightly Load" --verbose
  tm1cli chores run "Nightly Load" --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runChoresRun,
}

func runChoresRun(cmd *cobra.Command, args []string) error {
	choreName := args[0]

	if !choreRunAsync && choreRunTimeout <= 0 {
		emitChoreError(choreName, "error", "", "--timeout must be greater than zero.", isJSONOutput(nil))
		return errSilent
	}

	cfg, err := loadConfig()
	if err != nil {
		emitChoreError(choreName, "error", "", err.Error(), isJSONOutput(nil))
		return errSilent
	}
	jsonMode := isJSONOutput(cfg)

	cl, err := createClient(cfg)
	if err != nil {
		emitChoreError(choreName, "error", "", err.Error(), jsonMode)
		return errSilent
	}

	if flagVerbose {
		if err := printChoreTasksPreflight(cl, choreName); err != nil {
			fmt.Fprintf(os.Stderr, "[warn] cannot fetch chore tasks: %s\n", err)
		}
	}

	endpoint := choreEndpoint(choreName, "tm1.Execute")

	if choreRunAsync {
		return runChoreAsync(cl, choreName, endpoint, jsonMode)
	}
	return runChoreSync(cl, choreName, endpoint, jsonMode)
}

func runChoreAsync(cl *client.Client, choreName, endpoint string, jsonMode bool) error {
	threadID, err := cl.PostAsync(endpoint, map[string]interface{}{})
	if err != nil {
		return handleChoreRunError(err, choreName, "", jsonMode)
	}
	result := model.ChoreRunResult{
		Chore:    choreName,
		Status:   "started",
		ThreadID: threadID,
		Message:  fmt.Sprintf("Chore '%s' started asynchronously.", choreName),
	}
	if jsonMode {
		output.PrintJSON(result)
	} else {
		fmt.Printf("Chore '%s' started asynchronously.\n", choreName)
		fmt.Printf("  Thread ID: %s\n", threadID)
	}
	return nil
}

func runChoreSync(cl *client.Client, choreName, endpoint string, jsonMode bool) error {
	cl.SetTimeout(choreRunTimeout) // MUST run before Post.

	start := time.Now()
	_, err := cl.Post(endpoint, map[string]interface{}{})
	elapsed := time.Since(start)

	if err != nil {
		return handleChoreRunError(err, choreName, choreRunTimeout.String(), jsonMode)
	}

	result := model.ChoreRunResult{
		Chore:      choreName,
		Status:     "completed",
		DurationMs: elapsed.Milliseconds(),
		Message:    fmt.Sprintf("Chore '%s' executed successfully.", choreName),
	}
	if jsonMode {
		output.PrintJSON(result)
	} else {
		fmt.Printf("Chore '%s' executed successfully.\n", choreName)
		fmt.Printf("  Status:   Completed\n")
		fmt.Printf("  Duration: %.1fs\n", elapsed.Seconds())
	}
	return nil
}

func handleChoreRunError(err error, choreName, timeout string, jsonMode bool) error {
	if errors.Is(err, client.ErrNotFound) {
		emitChoreError(choreName, "not_found", "",
			fmt.Sprintf("Chore '%s' not found.", choreName), jsonMode)
		return errExit(3)
	}
	if strings.HasPrefix(err.Error(), "HTTP 403") {
		emitChoreError(choreName, "forbidden", "",
			"Permission denied. Running chores requires admin privileges.", jsonMode)
		return errSilent
	}
	if errors.Is(err, client.ErrTimeout) {
		emitChoreError(choreName, "timeout", timeout,
			fmt.Sprintf("Chore '%s' did not complete within %s.", choreName, timeout), jsonMode)
		return errExit(4)
	}
	emitChoreError(choreName, "error", "", err.Error(), jsonMode)
	return errSilent
}

// emitChoreError unifies the error-output shape so JSON mode always emits a
// single ChoreRunResult on stdout (matching the success path) and text mode
// always emits a human-readable line on stderr.
func emitChoreError(choreName, status, timeout, message string, jsonMode bool) {
	if jsonMode {
		output.PrintJSON(model.ChoreRunResult{
			Chore:   choreName,
			Status:  status,
			Timeout: timeout,
			Message: message,
		})
		return
	}
	output.PrintError(message, false)
}

func printChoreTasksPreflight(cl *client.Client, choreName string) error {
	endpoint := choreEndpoint(choreName, "") +
		"?$select=Name&$expand=Tasks($expand=Process($select=Name))"
	data, err := cl.Get(endpoint)
	if err != nil {
		return err
	}
	var chore model.ChoreWithTasks
	if jsonErr := json.Unmarshal(data, &chore); jsonErr != nil {
		return fmt.Errorf("cannot parse chore: %w", jsonErr)
	}
	fmt.Fprintf(os.Stderr, "Chore '%s' has %d task(s):\n", chore.Name, len(chore.Tasks))
	for i, t := range chore.Tasks {
		step := t.Step
		if step == 0 {
			step = i + 1
		}
		fmt.Fprintf(os.Stderr, "  [%d] %s\n", step, t.Process.Name)
	}
	return nil
}

func init() {
	rootCmd.AddCommand(choresCmd)
	choresCmd.AddCommand(choresListCmd)
	choresCmd.AddCommand(choresShowCmd)
	choresCmd.AddCommand(choresActivateCmd)
	choresCmd.AddCommand(choresDeactivateCmd)
	choresCmd.AddCommand(choresRunCmd)

	choresListCmd.Flags().StringVar(&choresFilter, "filter", "", "Filter by name (case-insensitive, partial match)")
	choresListCmd.Flags().BoolVar(&choresActive, "active", false, "Show only active chores")
	choresListCmd.Flags().BoolVar(&choresInactive, "inactive", false, "Show only inactive chores")
	choresListCmd.Flags().IntVar(&choresLimit, "limit", 0, "Max results to show (default from settings)")
	choresListCmd.Flags().BoolVar(&choresAll, "all", false, "Show all results, no limit")
	choresListCmd.Flags().BoolVar(&choresShowSystem, "show-system", false, "Include system chores (names starting with })")

	choresActivateCmd.Flags().BoolVar(&choresActivateYes, "yes", false, "Skip confirmation prompt")
	choresActivateCmd.Flags().BoolVar(&choresActivateDryRun, "dry-run", false, "Preview the activate without sending it")

	choresDeactivateCmd.Flags().BoolVar(&choresDeactivateYes, "yes", false, "Skip confirmation prompt")
	choresDeactivateCmd.Flags().BoolVar(&choresDeactivateDryRun, "dry-run", false, "Preview the deactivate without sending it")

	choresRunCmd.Flags().BoolVar(&choreRunAsync, "async", false,
		"Return immediately with the server-side thread ID; do not wait")
	choresRunCmd.Flags().DurationVar(&choreRunTimeout, "timeout", defaultChoreTimeout,
		"Maximum time to wait for the chore to complete (sync mode only)")
}
