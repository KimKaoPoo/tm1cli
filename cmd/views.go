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
	viewsFilter string
	viewsLimit  int
	viewsAll    bool
	viewsCount  bool
)

var viewsCmd = &cobra.Command{
	Use:   "views <cube>",
	Short: "List views of a cube",
	Long: `List all views of a cube.

Equivalent to: Cube Viewer → Views in TM1 Architect
               or the Views panel in PAW
REST API:      GET /Cubes('name')/Views

Results are limited to 50 by default. Use --all to show everything.`,
	Example: `  tm1cli views SalesCube
  tm1cli views SalesCube --filter "actual"
  tm1cli views SalesCube --all
  tm1cli views SalesCube --count
  tm1cli views SalesCube --server production
  tm1cli views SalesCube --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runViews,
}

func runViews(cmd *cobra.Command, args []string) error {
	cubeName := args[0]

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

	limit := getLimit(cfg, viewsLimit, viewsAll)

	endpoint := fmt.Sprintf("Cubes('%s')/Views?$select=Name", url.PathEscape(cubeName))

	// Try server-side filter
	filterFallback := false
	if viewsFilter != "" {
		safeFilter := strings.ReplaceAll(viewsFilter, "'", "''")
		filterEndpoint := endpoint + fmt.Sprintf("&$filter=contains(tolower(Name),tolower('%s'))", safeFilter)
		data, err := cl.Get(filterEndpoint)
		if err == nil {
			var resp model.ViewResponse
			if jsonErr := json.Unmarshal(data, &resp); jsonErr == nil {
				displayViews(resp.Value, len(resp.Value), limit, jsonMode)
				return nil
			}
		}
		// Fallback to client-side filter
		filterFallback = true
		output.PrintWarning("Server-side filter not supported, filtering locally...")
	}

	// Fetch all (or with $top)
	fetchEndpoint := endpoint
	if limit > 0 && viewsFilter == "" {
		fetchEndpoint += fmt.Sprintf("&$top=%d", limit)
	}

	data, err := cl.Get(fetchEndpoint)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var resp model.ViewResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	views := resp.Value

	// Client-side filter
	if viewsFilter != "" && filterFallback {
		views = filterViewsByName(views, viewsFilter)
	}

	displayViews(views, len(views), limit, jsonMode)
	return nil
}

func filterViewsByName(views []model.View, filter string) []model.View {
	lower := strings.ToLower(filter)
	var filtered []model.View
	for _, v := range views {
		if strings.Contains(strings.ToLower(v.Name), lower) {
			filtered = append(filtered, v)
		}
	}
	return filtered
}

func displayViews(views []model.View, total int, limit int, jsonMode bool) {
	if viewsCount {
		if jsonMode {
			output.PrintJSON(map[string]int{"count": total})
		} else {
			fmt.Printf("%d views\n", total)
		}
		return
	}

	shown := views
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}

	if jsonMode {
		output.PrintJSON(shown)
		return
	}

	headers := []string{"NAME"}
	rows := make([][]string, len(shown))
	for i, v := range shown {
		rows[i] = []string{v.Name}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(shown), total)
}

func init() {
	rootCmd.AddCommand(viewsCmd)
	viewsCmd.Flags().StringVar(&viewsFilter, "filter", "", "Filter by name (case-insensitive, partial match)")
	viewsCmd.Flags().IntVar(&viewsLimit, "limit", 0, "Max results to show (default from settings)")
	viewsCmd.Flags().BoolVar(&viewsAll, "all", false, "Show all results, no limit")
	viewsCmd.Flags().BoolVar(&viewsCount, "count", false, "Show count only")
}
