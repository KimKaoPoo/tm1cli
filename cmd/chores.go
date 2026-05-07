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
	choresFilter   string
	choresActive   bool
	choresInactive bool
)

var choresCmd = &cobra.Command{
	Use:   "chores",
	Short: "View TM1 chores and schedules",
	Long:  `View TM1 chores (scheduled task chains) and their step-by-step task lists.`,
}

var choresListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all chores",
	Long: `List all chores on the TM1 server.

REST API: GET /Chores

Frequency is rendered human-readable in table mode (e.g. "Every 1 day"); JSON
mode preserves the raw ISO 8601 duration so it remains machine-readable.`,
	Example: `  tm1cli chores list
  tm1cli chores list --active
  tm1cli chores list --filter "load"
  tm1cli chores list --output json`,
	RunE: runChoresList,
}

var choresShowCmd = &cobra.Command{
	Use:   "show <name>",
	Short: "Show a chore's task list",
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
	return url.PathEscape(strings.ReplaceAll(name, "'", "''"))
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

	// Belt-and-suspenders: cobra's MarkFlagsMutuallyExclusive may not fire
	// when RunE is invoked directly in tests, so guard at runtime too.
	if choresActive && choresInactive {
		output.PrintError("--active and --inactive are mutually exclusive.", jsonMode)
		return errSilent
	}

	const base = "Chores?$select=Name,Active,StartTime,DSTSensitivity,Frequency&$expand=Tasks($select=Step)"

	chores, err := fetchChores(cl, base, choresFilter, jsonMode)
	if err != nil {
		return err
	}

	chores = filterChoresByActive(chores, choresActive, choresInactive)
	displayChores(chores, jsonMode)
	return nil
}

// fetchChores tries server-side $filter first, falling back to client-side
// filtering with a [warn] when the server rejects the filter.
func fetchChores(cl *client.Client, base, filter string, jsonMode bool) ([]model.Chore, error) {
	if filter == "" {
		chores, err := getChores(cl, base)
		if err != nil {
			output.PrintError(err.Error(), jsonMode)
			return nil, errSilent
		}
		return chores, nil
	}

	safe := strings.ReplaceAll(filter, "'", "''")
	v := url.Values{}
	v.Set("$filter", fmt.Sprintf("contains(tolower(Name),tolower('%s'))", safe))
	if chores, err := getChores(cl, base+"&"+v.Encode()); err == nil {
		return chores, nil
	}

	output.PrintWarning("Server-side filter not supported, filtering locally...")
	chores, err := getChores(cl, base)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return nil, errSilent
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

func displayChores(chores []model.Chore, jsonMode bool) {
	if jsonMode {
		if chores == nil {
			chores = []model.Chore{}
		}
		output.PrintJSON(chores)
		return
	}
	headers := []string{"NAME", "ACTIVE", "STARTTIME", "DSTSENSITIVITY", "FREQUENCY", "TASKS"}
	rows := make([][]string, len(chores))
	for i, c := range chores {
		rows[i] = []string{
			c.Name,
			strconv.FormatBool(c.Active),
			c.StartTime,
			strconv.FormatBool(c.DSTSensitivity),
			formatChoreFrequency(c.Frequency),
			strconv.Itoa(len(c.Tasks)),
		}
	}
	output.PrintTable(headers, rows)
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
	fmt.Printf("DSTSensitivity: %s\n", strconv.FormatBool(chore.DSTSensitivity))
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
}
