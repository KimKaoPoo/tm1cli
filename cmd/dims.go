package cmd

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

var (
	dimsFilter     string
	dimsLimit      int
	dimsAll        bool
	dimsShowSystem bool
	dimsCount      bool
)

var dimsCmd = &cobra.Command{
	Use:   "dims",
	Short: "List dimensions (Server Explorer → Dimensions in Architect)",
	Long: `List all dimensions on the TM1 server.

Equivalent to: Server Explorer → Dimensions in TM1 Architect
REST API:      GET /Dimensions

System dimensions (starting with }) are hidden by default.`,
	Example: `  tm1cli dims
  tm1cli dims --filter "period"
  tm1cli dims --all
  tm1cli dims --show-system
  tm1cli dims --output json`,
	RunE: runDims,
}

func runDims(cmd *cobra.Command, args []string) error {
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

	showSystem := getShowSystem(cfg, dimsShowSystem)
	limit := getLimit(cfg, dimsLimit, dimsAll)

	endpoint := "Dimensions?$select=Name"

	// Try server-side filter
	filterFallback := false
	if dimsFilter != "" {
		safeFilter := strings.ReplaceAll(dimsFilter, "'", "''")
		filterEndpoint := endpoint + fmt.Sprintf("&$filter=contains(tolower(Name),tolower('%s'))", safeFilter)
		data, err := cl.Get(filterEndpoint)
		if err == nil {
			var resp model.DimensionResponse
			if jsonErr := json.Unmarshal(data, &resp); jsonErr == nil {
				dims := filterSystemDims(resp.Value, showSystem)
				displayDims(dims, len(dims), limit, jsonMode)
				return nil
			}
		}
		filterFallback = true
		output.PrintWarning("Server-side filter not supported, filtering locally...")
	}

	fetchEndpoint := endpoint
	if limit > 0 && dimsFilter == "" {
		fetchEndpoint += fmt.Sprintf("&$top=%d", limit+500)
	}

	data, err := cl.Get(fetchEndpoint)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var resp model.DimensionResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	dims := filterSystemDims(resp.Value, showSystem)

	if dimsFilter != "" && filterFallback {
		dims = filterDimsByName(dims, dimsFilter)
	}

	displayDims(dims, len(dims), limit, jsonMode)
	return nil
}

func filterSystemDims(dims []model.Dimension, showSystem bool) []model.Dimension {
	if showSystem {
		return dims
	}
	var filtered []model.Dimension
	for _, d := range dims {
		if !strings.HasPrefix(d.Name, "}") {
			filtered = append(filtered, d)
		}
	}
	return filtered
}

func filterDimsByName(dims []model.Dimension, filter string) []model.Dimension {
	lower := strings.ToLower(filter)
	var filtered []model.Dimension
	for _, d := range dims {
		if strings.Contains(strings.ToLower(d.Name), lower) {
			filtered = append(filtered, d)
		}
	}
	return filtered
}

func displayDims(dims []model.Dimension, total int, limit int, jsonMode bool) {
	if dimsCount {
		if jsonMode {
			output.PrintJSON(map[string]int{"count": total})
		} else {
			fmt.Printf("%d dimensions\n", total)
		}
		return
	}

	shown := dims
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}

	if jsonMode {
		output.PrintJSON(shown)
		return
	}

	headers := []string{"NAME"}
	rows := make([][]string, len(shown))
	for i, d := range shown {
		rows[i] = []string{d.Name}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(shown), total)
}

// --- dims members ---

var (
	membersHierarchy string
	membersFilter    string
	membersLimit     int
	membersAll       bool
	membersCount     bool
)

var dimsMembersCmd = &cobra.Command{
	Use:   "members <dimension>",
	Short: "List elements (members) of a dimension",
	Long: `List elements (members) of a dimension.

Equivalent to: Dimension Editor in TM1 Architect
               or expanding a dimension in PAW
REST API:      GET /Dimensions('name')/Hierarchies('name')/Elements

By default uses the dimension's same-named hierarchy.
Use --hierarchy for alternate hierarchies.`,
	Example: `  tm1cli dims members Period
  tm1cli dims members Region --hierarchy "Alternate Region"
  tm1cli dims members Period --filter "Q"
  tm1cli dims members Account --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runDimsMembers,
}

func runDimsMembers(cmd *cobra.Command, args []string) error {
	dimName := args[0]

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

	limit := getLimit(cfg, membersLimit, membersAll)

	hierarchy := membersHierarchy
	if hierarchy == "" {
		hierarchy = dimName
	}

	endpoint := fmt.Sprintf("Dimensions('%s')/Hierarchies('%s')/Elements?$select=Name,Type", url.PathEscape(dimName), url.PathEscape(hierarchy))

	// Try server-side filter
	filterFallback := false
	if membersFilter != "" {
		safeFilter := strings.ReplaceAll(membersFilter, "'", "''")
		filterEndpoint := endpoint + fmt.Sprintf("&$filter=contains(tolower(Name),tolower('%s'))", safeFilter)
		data, err := cl.Get(filterEndpoint)
		if err == nil {
			var resp model.ElementResponse
			if jsonErr := json.Unmarshal(data, &resp); jsonErr == nil {
				displayMembers(resp.Value, len(resp.Value), limit, jsonMode)
				return nil
			}
		}
		filterFallback = true
		output.PrintWarning("Server-side filter not supported, filtering locally...")
	}

	fetchEndpoint := endpoint
	if limit > 0 && membersFilter == "" {
		fetchEndpoint += fmt.Sprintf("&$top=%d", limit)
	}

	data, err := cl.Get(fetchEndpoint)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var resp model.ElementResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	elements := resp.Value

	if membersFilter != "" && filterFallback {
		elements = filterElementsByName(elements, membersFilter)
	}

	displayMembers(elements, len(elements), limit, jsonMode)
	return nil
}

func filterElementsByName(elements []model.Element, filter string) []model.Element {
	lower := strings.ToLower(filter)
	var filtered []model.Element
	for _, e := range elements {
		if strings.Contains(strings.ToLower(e.Name), lower) {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

func displayMembers(elements []model.Element, total int, limit int, jsonMode bool) {
	if membersCount {
		if jsonMode {
			output.PrintJSON(map[string]int{"count": total})
		} else {
			fmt.Printf("%d members\n", total)
		}
		return
	}

	shown := elements
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}

	if jsonMode {
		output.PrintJSON(shown)
		return
	}

	headers := []string{"NAME", "TYPE"}
	rows := make([][]string, len(shown))
	for i, e := range shown {
		rows[i] = []string{e.Name, e.Type}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(shown), total)
}

func init() {
	rootCmd.AddCommand(dimsCmd)
	dimsCmd.AddCommand(dimsMembersCmd)

	dimsCmd.Flags().StringVar(&dimsFilter, "filter", "", "Filter by name (case-insensitive, partial match)")
	dimsCmd.Flags().IntVar(&dimsLimit, "limit", 0, "Max results to show (default from settings)")
	dimsCmd.Flags().BoolVar(&dimsAll, "all", false, "Show all results, no limit")
	dimsCmd.Flags().BoolVar(&dimsShowSystem, "show-system", false, "Include control dimensions (names starting with })")
	dimsCmd.Flags().BoolVar(&dimsCount, "count", false, "Show count only")

	dimsMembersCmd.Flags().StringVar(&membersHierarchy, "hierarchy", "", "Hierarchy name (default: same as dimension name)")
	dimsMembersCmd.Flags().StringVar(&membersFilter, "filter", "", "Filter by name (case-insensitive, partial match)")
	dimsMembersCmd.Flags().IntVar(&membersLimit, "limit", 0, "Max results to show (default from settings)")
	dimsMembersCmd.Flags().BoolVar(&membersAll, "all", false, "Show all members, no limit")
	dimsMembersCmd.Flags().BoolVar(&membersCount, "count", false, "Show count only")
}
