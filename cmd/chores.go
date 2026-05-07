package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"tm1cli/internal/client"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

var (
	choresFilter     string
	choresActive     bool
	choresInactive   bool
	choresLimit      int
	choresAll        bool
	choresShowSystem bool
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

	// $top is only safe to apply server-side when no client-side filters
	// will further trim results — otherwise we may silently omit matches.
	fetchEndpoint := base
	clientSideFilters := choresFilter != "" || !showSystem || choresActive || choresInactive
	if limit > 0 && !clientSideFilters {
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

func init() {
	rootCmd.AddCommand(choresCmd)
	choresCmd.AddCommand(choresListCmd)
	choresCmd.AddCommand(choresShowCmd)

	choresListCmd.Flags().StringVar(&choresFilter, "filter", "", "Filter by name (case-insensitive, partial match)")
	choresListCmd.Flags().BoolVar(&choresActive, "active", false, "Show only active chores")
	choresListCmd.Flags().BoolVar(&choresInactive, "inactive", false, "Show only inactive chores")
	choresListCmd.Flags().IntVar(&choresLimit, "limit", 0, "Max results to show (default from settings)")
	choresListCmd.Flags().BoolVar(&choresAll, "all", false, "Show all results, no limit")
	choresListCmd.Flags().BoolVar(&choresShowSystem, "show-system", false, "Include system chores (names starting with })")
}
