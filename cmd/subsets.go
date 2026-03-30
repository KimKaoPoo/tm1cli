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
	subsetsFilter    string
	subsetsLimit     int
	subsetsAll       bool
	subsetsCount     bool
	subsetsHierarchy string
)

var subsetsCmd = &cobra.Command{
	Use:   "subsets <dimension>",
	Short: "List subsets of a dimension",
	Long: `List all subsets of a dimension.

Equivalent to: Dimension Editor → Subsets in TM1 Architect
               or the Subsets panel in PAW
REST API:      GET /Dimensions('name')/Hierarchies('name')/Subsets

By default uses the dimension's same-named hierarchy.
Use --hierarchy for alternate hierarchies.`,
	Example: `  tm1cli subsets Region
  tm1cli subsets Region --hierarchy "Alternate Region"
  tm1cli subsets Period --filter "quarter"
  tm1cli subsets Region --count
  tm1cli subsets Region --server production
  tm1cli subsets Region --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runSubsets,
}

func runSubsets(cmd *cobra.Command, args []string) error {
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

	limit := getLimit(cfg, subsetsLimit, subsetsAll)

	hierarchy := subsetsHierarchy
	if hierarchy == "" {
		hierarchy = dimName
	}

	endpoint := fmt.Sprintf("Dimensions('%s')/Hierarchies('%s')/Subsets?$select=Name", url.PathEscape(dimName), url.PathEscape(hierarchy))

	// Try server-side filter
	filterFallback := false
	if subsetsFilter != "" {
		safeFilter := strings.ReplaceAll(subsetsFilter, "'", "''")
		filterEndpoint := endpoint + fmt.Sprintf("&$filter=contains(tolower(Name),tolower('%s'))", safeFilter)
		data, err := cl.Get(filterEndpoint)
		if err == nil {
			var resp model.SubsetResponse
			if jsonErr := json.Unmarshal(data, &resp); jsonErr == nil {
				displaySubsets(resp.Value, len(resp.Value), limit, jsonMode)
				return nil
			}
		}
		// Fallback to client-side filter
		filterFallback = true
		output.PrintWarning("Server-side filter not supported, filtering locally...")
	}

	// Fetch all (or with $top)
	fetchEndpoint := endpoint
	if limit > 0 && subsetsFilter == "" {
		fetchEndpoint += fmt.Sprintf("&$top=%d", limit)
	}

	data, err := cl.Get(fetchEndpoint)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var resp model.SubsetResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	subsets := resp.Value

	// Client-side filter
	if subsetsFilter != "" && filterFallback {
		subsets = filterSubsetsByName(subsets, subsetsFilter)
	}

	displaySubsets(subsets, len(subsets), limit, jsonMode)
	return nil
}

func filterSubsetsByName(subsets []model.Subset, filter string) []model.Subset {
	lower := strings.ToLower(filter)
	var filtered []model.Subset
	for _, s := range subsets {
		if strings.Contains(strings.ToLower(s.Name), lower) {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

func displaySubsets(subsets []model.Subset, total int, limit int, jsonMode bool) {
	if subsetsCount {
		if jsonMode {
			output.PrintJSON(map[string]int{"count": total})
		} else {
			fmt.Printf("%d subsets\n", total)
		}
		return
	}

	shown := subsets
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}

	if jsonMode {
		output.PrintJSON(shown)
		return
	}

	headers := []string{"NAME"}
	rows := make([][]string, len(shown))
	for i, s := range shown {
		rows[i] = []string{s.Name}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(shown), total)
}

func init() {
	rootCmd.AddCommand(subsetsCmd)
	subsetsCmd.Flags().StringVar(&subsetsFilter, "filter", "", "Filter by name (case-insensitive, partial match)")
	subsetsCmd.Flags().IntVar(&subsetsLimit, "limit", 0, "Max results to show (default from settings)")
	subsetsCmd.Flags().BoolVar(&subsetsAll, "all", false, "Show all results, no limit")
	subsetsCmd.Flags().BoolVar(&subsetsCount, "count", false, "Show count only")
	subsetsCmd.Flags().StringVar(&subsetsHierarchy, "hierarchy", "", "Hierarchy name (default: same as dimension name)")
}
