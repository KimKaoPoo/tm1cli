package cmd

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

var (
	sandboxListFilter string
	sandboxListLimit  int
	sandboxListAll    bool
	sandboxListCount  bool
	sandboxListLoaded bool
	sandboxListActive bool
)

var sandboxCmd = &cobra.Command{
	Use:   "sandbox",
	Short: "Manage TM1 sandboxes",
	Long:  `Manage and inspect TM1 sandboxes.`,
}

var sandboxListCmd = &cobra.Command{
	Use:   "list",
	Short: "List TM1 sandboxes",
	Long: `List sandboxes on the TM1 server.

REST API: GET /Sandboxes

Results are limited to 50 by default. Use --all to show everything.`,
	Example: `  tm1cli sandbox list
  tm1cli sandbox list --loaded
  tm1cli sandbox list --active
  tm1cli sandbox list --filter "fy24"
  tm1cli sandbox list --output json`,
	Args: cobra.NoArgs,
	RunE: runSandboxList,
}

var sandboxCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new sandbox",
	Long: `Create a new TM1 sandbox.

REST API: POST /Sandboxes`,
	Example: `  tm1cli sandbox create FY24Plan
  tm1cli sandbox create FY24Plan --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runSandboxCreate,
}

func runSandboxList(cmd *cobra.Command, args []string) error {
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

	limit := getLimit(cfg, sandboxListLimit, sandboxListAll)
	activeFilters := sandboxListFilter != "" || sandboxListLoaded || sandboxListActive
	endpoint := "Sandboxes?$select=Name,IncludeInSandboxDimension,Loaded,Active,Queued"

	filterFallback := false
	if sandboxListFilter != "" {
		safeFilter := strings.ReplaceAll(sandboxListFilter, "'", "''")
		filterEndpoint := endpoint + fmt.Sprintf("&$filter=contains(tolower(Name),tolower('%s'))", safeFilter)
		data, err := cl.Get(filterEndpoint)
		if err == nil {
			var resp model.SandboxResponse
			if jsonErr := json.Unmarshal(data, &resp); jsonErr == nil {
				sandboxes := applySandboxBoolFilters(resp.Value, sandboxListLoaded, sandboxListActive)
				displaySandboxes(sandboxes, len(sandboxes), limit, jsonMode)
				return nil
			}
		}
		filterFallback = true
		output.PrintWarning("Server-side filter not supported, filtering locally...")
	}

	fetchEndpoint := endpoint
	if limit > 0 && !activeFilters {
		fetchEndpoint += fmt.Sprintf("&$top=%d", limit)
	}

	data, err := cl.Get(fetchEndpoint)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}
	var resp model.SandboxResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	sandboxes := resp.Value
	if sandboxListFilter != "" && filterFallback {
		sandboxes = filterSandboxesByName(sandboxes, sandboxListFilter)
	}
	sandboxes = applySandboxBoolFilters(sandboxes, sandboxListLoaded, sandboxListActive)
	displaySandboxes(sandboxes, len(sandboxes), limit, jsonMode)
	return nil
}

func filterSandboxesByName(sandboxes []model.Sandbox, filter string) []model.Sandbox {
	lower := strings.ToLower(filter)
	var out []model.Sandbox
	for _, s := range sandboxes {
		if strings.Contains(strings.ToLower(s.Name), lower) {
			out = append(out, s)
		}
	}
	return out
}

func applySandboxBoolFilters(sandboxes []model.Sandbox, loaded, active bool) []model.Sandbox {
	if !loaded && !active {
		return sandboxes
	}
	var out []model.Sandbox
	for _, s := range sandboxes {
		if loaded && !s.Loaded {
			continue
		}
		if active && !s.Active {
			continue
		}
		out = append(out, s)
	}
	return out
}

func displaySandboxes(sandboxes []model.Sandbox, total int, limit int, jsonMode bool) {
	if sandboxListCount {
		if jsonMode {
			output.PrintJSON(map[string]int{"count": total})
		} else {
			fmt.Printf("%d sandboxes\n", total)
		}
		return
	}

	shown := sandboxes
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}
	if jsonMode {
		output.PrintJSON(shown)
		return
	}

	headers := []string{"NAME", "IN SANDBOX DIM", "LOADED", "ACTIVE", "QUEUED"}
	rows := make([][]string, len(shown))
	for i, s := range shown {
		rows[i] = []string{
			s.Name,
			strconv.FormatBool(s.IncludeInSandboxDimension),
			strconv.FormatBool(s.Loaded),
			strconv.FormatBool(s.Active),
			strconv.FormatBool(s.Queued),
		}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(shown), total, "--filter, --loaded, --active, or --all")
}

func runSandboxCreate(cmd *cobra.Command, args []string) error {
	name := args[0]

	cfg, err := loadConfig()
	if err != nil {
		output.PrintError(err.Error(), isJSONOutput(nil))
		return errSilent
	}
	jsonMode := isJSONOutput(cfg)

	if strings.TrimSpace(name) == "" {
		output.PrintError("Sandbox name cannot be empty.", jsonMode)
		return errSilent
	}

	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	body, err := cl.Post("Sandboxes", map[string]string{"Name": name})
	if err != nil {
		if isDuplicateSandboxError(body, err) {
			output.PrintError(fmt.Sprintf("Sandbox '%s' already exists.", name), jsonMode)
			return errSilent
		}
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	if jsonMode {
		output.PrintJSON(map[string]string{"status": "created", "name": name})
	} else {
		fmt.Printf("Created sandbox '%s'.\n", name)
	}
	return nil
}

// isDuplicateSandboxError reports whether the (body, err) pair from a
// POST /Sandboxes failure is a duplicate-name conflict. TM1 returns HTTP
// 400 (and HTTP 409 on some newer PA versions) with a body containing
// "already exists" or "duplicate". The body is inspected (rather than
// only err.Error()) because client.httpError truncates the body at 200
// chars in the error string, which may cut off the keyword.
func isDuplicateSandboxError(body []byte, err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if !strings.HasPrefix(msg, "HTTP 400") && !strings.HasPrefix(msg, "HTTP 409") {
		return false
	}
	combined := strings.ToLower(msg + " " + string(body))
	return strings.Contains(combined, "already exists") || strings.Contains(combined, "duplicate")
}

func init() {
	rootCmd.AddCommand(sandboxCmd)
	sandboxCmd.AddCommand(sandboxListCmd)
	sandboxCmd.AddCommand(sandboxCreateCmd)

	sandboxListCmd.Flags().StringVar(&sandboxListFilter, "filter", "", "Filter by name (case-insensitive, partial match)")
	sandboxListCmd.Flags().IntVar(&sandboxListLimit, "limit", 0, "Max results to show (default from settings)")
	sandboxListCmd.Flags().BoolVar(&sandboxListAll, "all", false, "Show all results, no limit")
	sandboxListCmd.Flags().BoolVar(&sandboxListCount, "count", false, "Show count only")
	sandboxListCmd.Flags().BoolVar(&sandboxListLoaded, "loaded", false, "Show only sandboxes currently loaded in memory")
	sandboxListCmd.Flags().BoolVar(&sandboxListActive, "active", false, "Show only sandboxes the current user has active")
}
