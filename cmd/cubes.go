package cmd

import (
	"encoding/json"
	"fmt"
	"strings"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

var (
	cubesFilter     string
	cubesLimit      int
	cubesAll        bool
	cubesShowSystem bool
	cubesCount      bool
)

var cubesCmd = &cobra.Command{
	Use:   "cubes",
	Short: "List cubes (Server Explorer → Cubes in Architect)",
	Long: `List all cubes on the TM1 server.

Equivalent to: Server Explorer → Cubes in TM1 Architect
REST API:      GET /Cubes

System cubes (starting with }) are hidden by default. Use --show-system to include them.
Results are limited to 50 by default. Use --all to show everything.`,
	Example: `  tm1cli cubes
  tm1cli cubes --filter "ledger"
  tm1cli cubes --all
  tm1cli cubes --show-system
  tm1cli cubes --all --show-system
  tm1cli cubes --count
  tm1cli cubes --server production
  tm1cli cubes --output json`,
	RunE: runCubes,
}

func runCubes(cmd *cobra.Command, args []string) error {
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

	showSystem := getShowSystem(cfg, cubesShowSystem)
	limit := getLimit(cfg, cubesLimit, cubesAll)

	// Build endpoint
	endpoint := "Cubes?$select=Name,LastDataUpdate"

	// Try server-side filter
	filterFallback := false
	if cubesFilter != "" {
		safeFilter := strings.ReplaceAll(cubesFilter, "'", "''")
		filterEndpoint := endpoint + fmt.Sprintf("&$filter=contains(tolower(Name),tolower('%s'))", safeFilter)
		data, err := cl.Get(filterEndpoint)
		if err == nil {
			var resp model.CubeResponse
			if jsonErr := json.Unmarshal(data, &resp); jsonErr == nil {
				cubes := filterSystemCubes(resp.Value, showSystem)
				displayCubes(cubes, len(cubes), limit, jsonMode)
				return nil
			}
		}
		// Fallback to client-side filter
		filterFallback = true
		output.PrintWarning("Server-side filter not supported, filtering locally...")
	}

	// Fetch all (or with $top)
	fetchEndpoint := endpoint
	if limit > 0 && cubesFilter == "" {
		// When filtering, we need all results for client-side filter
		fetchEndpoint += fmt.Sprintf("&$top=%d", limit+500) // fetch extra to account for system objects
	}

	data, err := cl.Get(fetchEndpoint)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var resp model.CubeResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	cubes := filterSystemCubes(resp.Value, showSystem)

	// Client-side filter
	if cubesFilter != "" && filterFallback {
		cubes = filterCubesByName(cubes, cubesFilter)
	}

	displayCubes(cubes, len(cubes), limit, jsonMode)
	return nil
}

func filterSystemCubes(cubes []model.Cube, showSystem bool) []model.Cube {
	if showSystem {
		return cubes
	}
	var filtered []model.Cube
	for _, c := range cubes {
		if !strings.HasPrefix(c.Name, "}") {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

func filterCubesByName(cubes []model.Cube, filter string) []model.Cube {
	lower := strings.ToLower(filter)
	var filtered []model.Cube
	for _, c := range cubes {
		if strings.Contains(strings.ToLower(c.Name), lower) {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

func displayCubes(cubes []model.Cube, total int, limit int, jsonMode bool) {
	if cubesCount {
		if jsonMode {
			output.PrintJSON(map[string]int{"count": total})
		} else {
			fmt.Printf("%d cubes\n", total)
		}
		return
	}

	shown := cubes
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}

	if jsonMode {
		output.PrintJSON(shown)
		return
	}

	headers := []string{"NAME", "LAST UPDATED"}
	rows := make([][]string, len(shown))
	for i, c := range shown {
		lastUpdate := c.LastDataUpdate
		if lastUpdate == "" {
			lastUpdate = "-"
		}
		rows[i] = []string{c.Name, lastUpdate}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(shown), total)
}

func init() {
	rootCmd.AddCommand(cubesCmd)
	cubesCmd.Flags().StringVar(&cubesFilter, "filter", "", "Filter by name (case-insensitive, partial match)")
	cubesCmd.Flags().IntVar(&cubesLimit, "limit", 0, "Max results to show (default from settings)")
	cubesCmd.Flags().BoolVar(&cubesAll, "all", false, "Show all results, no limit")
	cubesCmd.Flags().BoolVar(&cubesShowSystem, "show-system", false, "Include system cubes (names starting with })")
	cubesCmd.Flags().BoolVar(&cubesCount, "count", false, "Show count only")
}
