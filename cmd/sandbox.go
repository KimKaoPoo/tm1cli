package cmd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"tm1cli/internal/client"
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

	sandboxMergeYes    bool
	sandboxMergeClean  bool
	sandboxMergeTarget string
	sandboxMergeDryRun bool

	sandboxDeleteYes    bool
	sandboxDeleteDryRun bool
)

var sandboxCmd = &cobra.Command{
	Use:          "sandbox",
	Short:        "Manage TM1 sandboxes",
	Long:         `Manage and inspect TM1 sandboxes.`,
	SilenceUsage: true,
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
	Args:         cobra.NoArgs,
	RunE:         runSandboxList,
	SilenceUsage: true,
}

var sandboxCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new sandbox",
	Long: `Create a new TM1 sandbox.

REST API: POST /Sandboxes`,
	Example: `  tm1cli sandbox create FY24Plan
  tm1cli sandbox create FY24Plan --output json`,
	Args:         cobra.ExactArgs(1),
	RunE:         runSandboxCreate,
	SilenceUsage: true,
}

var sandboxMergeCmd = &cobra.Command{
	Use:   "merge <name>",
	Short: "Merge a sandbox into the base (or another sandbox)",
	Long: `Merge a TM1 sandbox into the base sandbox, or into another named sandbox via --target.

REST API: POST /Sandboxes('name')/tm1.Merge

WARNING: merge applies the source sandbox's pending writes to the target.
Once merged, the target reflects the combined state. Use --clean to drop
the source sandbox after a successful merge.

The command prompts for confirmation by default. Use --yes to skip the
prompt for scripting. --dry-run takes precedence over --yes.

Exit codes:
  0  merge completed
  1  generic error (auth, network, server, conflict, sandbox loaded by another user)
  3  sandbox not found`,
	Example: `  tm1cli sandbox merge FY24Plan
  tm1cli sandbox merge FY24Plan --yes
  tm1cli sandbox merge FY24Plan --target FY24Forecast
  tm1cli sandbox merge FY24Plan --clean --yes
  tm1cli sandbox merge FY24Plan --dry-run`,
	Args:         cobra.ExactArgs(1),
	RunE:         runSandboxMerge,
	SilenceUsage: true,
}

var sandboxDeleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a TM1 sandbox",
	Long: `Delete a TM1 sandbox by name.

REST API: DELETE /Sandboxes('name')

WARNING: deletion is permanent. Any unmerged writes in the sandbox are lost.

The command prompts for confirmation by default. Use --yes to skip the
prompt for scripting. --dry-run takes precedence over --yes.

Exit codes:
  0  sandbox deleted
  1  generic error (auth, network, server, sandbox loaded by another user)
  3  sandbox not found`,
	Example: `  tm1cli sandbox delete FY24Plan
  tm1cli sandbox delete FY24Plan --yes
  tm1cli sandbox delete FY24Plan --dry-run --output json`,
	Args:         cobra.ExactArgs(1),
	RunE:         runSandboxDelete,
	SilenceUsage: true,
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
	endpoint := "Sandboxes?$select=Name,IncludeInSandboxDimension,IsLoaded,IsActive,IsQueued"

	filterFallback := false
	if sandboxListFilter != "" {
		filterEndpoint := endpoint + fmt.Sprintf("&$filter=contains(tolower(Name),tolower('%s'))", odataEscape(sandboxListFilter))
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
	if limit > 0 && !activeFilters && !sandboxListCount {
		// Over-fetch above --limit so PrintSummary can report "Showing N of M"
		// when the server has more than --limit rows; otherwise the truncation
		// summary would never fire. Skip $top entirely when --count is set so
		// the displayed total is the real server total, not the over-fetch cap.
		fetchEndpoint += fmt.Sprintf("&$top=%d", limit+500)
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
		if loaded && !s.IsLoaded {
			continue
		}
		if active && !s.IsActive {
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
		if shown == nil {
			shown = []model.Sandbox{}
		}
		output.PrintJSON(shown)
		return
	}

	headers := []string{"NAME", "IN SANDBOX DIM", "LOADED", "ACTIVE", "QUEUED"}
	rows := make([][]string, len(shown))
	for i, s := range shown {
		rows[i] = []string{
			s.Name,
			strconv.FormatBool(s.IncludeInSandboxDimension),
			strconv.FormatBool(s.IsLoaded),
			strconv.FormatBool(s.IsActive),
			strconv.FormatBool(s.IsQueued),
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

func runSandboxMerge(cmd *cobra.Command, args []string) error {
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

	target := strings.TrimSpace(sandboxMergeTarget)
	if target == name {
		output.PrintError("--target cannot equal the source sandbox name.", jsonMode)
		return errSilent
	}

	targetLabel := "base"
	if target != "" {
		targetLabel = fmt.Sprintf("sandbox '%s'", target)
	}

	if sandboxMergeDryRun {
		if jsonMode {
			output.PrintJSON(map[string]interface{}{
				"status": "dry-run",
				"source": name,
				"target": target,
				"clean":  sandboxMergeClean,
			})
		} else {
			cleanPart := ""
			if sandboxMergeClean {
				cleanPart = " and clean source after merge"
			}
			fmt.Printf("[dry-run] Would merge sandbox '%s' into %s%s.\n", name, targetLabel, cleanPart)
		}
		return nil
	}

	if !sandboxMergeYes {
		cleanPart := ""
		if sandboxMergeClean {
			cleanPart = " (source will be cleaned after merge)"
		}
		fmt.Fprintf(os.Stderr, "About to merge sandbox '%s' into %s%s.\n", name, targetLabel, cleanPart)
		if !promptYesNo(bufio.NewReader(os.Stdin), "Continue?") {
			return nil
		}
	}

	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	payload := map[string]interface{}{
		"Source@odata.bind": fmt.Sprintf("Sandboxes('%s')", odataEscape(name)),
		"Target@odata.bind": fmt.Sprintf("Sandboxes('%s')", odataEscape(target)),
		"CleanAfter":        sandboxMergeClean,
	}

	endpoint := fmt.Sprintf("Sandboxes('%s')/tm1.Merge", odataKey(name))
	body, postErr := cl.Post(endpoint, payload)
	if postErr != nil {
		return handleSandboxMergeError(postErr, body, name, jsonMode)
	}

	if jsonMode {
		output.PrintJSON(map[string]interface{}{
			"status": "merged",
			"source": name,
			"target": target,
			"clean":  sandboxMergeClean,
		})
	} else {
		cleanPart := ""
		if sandboxMergeClean {
			cleanPart = " (source cleaned)"
		}
		fmt.Printf("Merged sandbox '%s' into %s%s.\n", name, targetLabel, cleanPart)
	}
	return nil
}

func runSandboxDelete(cmd *cobra.Command, args []string) error {
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

	if sandboxDeleteDryRun {
		if jsonMode {
			output.PrintJSON(map[string]string{"status": "dry-run", "name": name})
		} else {
			fmt.Printf("[dry-run] Would delete sandbox '%s'.\n", name)
		}
		return nil
	}

	if !sandboxDeleteYes {
		fmt.Fprintf(os.Stderr, "About to permanently delete sandbox '%s'. Unmerged writes will be lost.\n", name)
		if !promptYesNo(bufio.NewReader(os.Stdin), "Continue?") {
			return nil
		}
	}

	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	endpoint := fmt.Sprintf("Sandboxes('%s')", odataKey(name))
	if delErr := cl.Delete(endpoint); delErr != nil {
		return handleSandboxDeleteError(delErr, name, jsonMode)
	}

	if jsonMode {
		output.PrintJSON(map[string]string{"status": "deleted", "name": name})
	} else {
		fmt.Printf("Deleted sandbox '%s'.\n", name)
	}
	return nil
}

// handleSandboxMergeError funnels merge failures through one place. The
// "loaded by another user" check runs before the merge-conflict check
// because it's a more specific failure mode — TM1 may return a body
// containing both keywords, and the lock-message is more actionable.
func handleSandboxMergeError(err error, body []byte, name string, jsonMode bool) error {
	if errors.Is(err, client.ErrNotFound) {
		output.PrintError(fmt.Sprintf("Sandbox '%s' not found.", name), jsonMode)
		return errExit(3)
	}
	if strings.HasPrefix(err.Error(), "HTTP 403") {
		output.PrintError("Permission denied. Merging sandboxes requires admin or owner privileges.", jsonMode)
		return errSilent
	}
	if isSandboxLockedError(body, err) {
		output.PrintError(fmt.Sprintf("Sandbox '%s' is loaded by another user. Wait for it to be unloaded and retry.", name), jsonMode)
		return errSilent
	}
	if isSandboxMergeConflictError(body, err) {
		output.PrintError(fmt.Sprintf("Merge conflict on sandbox '%s'. Resolve conflicting changes and retry.", name), jsonMode)
		return errSilent
	}
	output.PrintError(err.Error(), jsonMode)
	return errSilent
}

func handleSandboxDeleteError(err error, name string, jsonMode bool) error {
	if errors.Is(err, client.ErrNotFound) {
		output.PrintError(fmt.Sprintf("Sandbox '%s' not found.", name), jsonMode)
		return errExit(3)
	}
	if strings.HasPrefix(err.Error(), "HTTP 403") {
		output.PrintError("Permission denied. Deleting a sandbox requires admin or owner privileges.", jsonMode)
		return errSilent
	}
	if isSandboxLockedError(nil, err) {
		output.PrintError(fmt.Sprintf("Sandbox '%s' is loaded by another user. Wait for it to be unloaded and retry.", name), jsonMode)
		return errSilent
	}
	output.PrintError(err.Error(), jsonMode)
	return errSilent
}

// isSandboxLockedError reports whether (body, err) indicates the sandbox
// is loaded by another user. TM1 returns HTTP 400/409 with messages
// like "Sandbox 'X' is loaded by user 'Y'", "currently loaded", or
// "is in use". The body is inspected (not only err.Error()) because
// client.httpError truncates at 200 chars, which can cut off the keyword.
// body may be nil when only the error string is available (e.g. DELETE).
func isSandboxLockedError(body []byte, err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if !strings.HasPrefix(msg, "HTTP 400") && !strings.HasPrefix(msg, "HTTP 409") {
		return false
	}
	combined := strings.ToLower(msg + " " + string(body))
	return strings.Contains(combined, "loaded by") ||
		strings.Contains(combined, "currently loaded") ||
		strings.Contains(combined, "is in use")
}

// isSandboxMergeConflictError detects merge-conflict responses. The
// keyword set is intentionally narrower than isSandboxLockedError so the
// more-specific locked-by-user message takes precedence when both apply.
func isSandboxMergeConflictError(body []byte, err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if !strings.HasPrefix(msg, "HTTP 400") && !strings.HasPrefix(msg, "HTTP 409") {
		return false
	}
	combined := strings.ToLower(msg + " " + string(body))
	return strings.Contains(combined, "conflict") ||
		strings.Contains(combined, "cannot be merged")
}

func init() {
	rootCmd.AddCommand(sandboxCmd)
	sandboxCmd.AddCommand(sandboxListCmd)
	sandboxCmd.AddCommand(sandboxCreateCmd)
	sandboxCmd.AddCommand(sandboxMergeCmd)
	sandboxCmd.AddCommand(sandboxDeleteCmd)

	sandboxListCmd.Flags().StringVar(&sandboxListFilter, "filter", "", "Filter by name (case-insensitive, partial match)")
	sandboxListCmd.Flags().IntVar(&sandboxListLimit, "limit", 0, "Max results to show (default from settings)")
	sandboxListCmd.Flags().BoolVar(&sandboxListAll, "all", false, "Show all results, no limit")
	sandboxListCmd.Flags().BoolVar(&sandboxListCount, "count", false, "Show count only")
	sandboxListCmd.Flags().BoolVar(&sandboxListLoaded, "loaded", false, "Show only sandboxes currently loaded in memory")
	sandboxListCmd.Flags().BoolVar(&sandboxListActive, "active", false, "Show only sandboxes the current user has active")

	sandboxMergeCmd.Flags().BoolVar(&sandboxMergeYes, "yes", false, "Skip confirmation prompt")
	sandboxMergeCmd.Flags().BoolVar(&sandboxMergeClean, "clean", false, "Delete the source sandbox after a successful merge")
	sandboxMergeCmd.Flags().StringVar(&sandboxMergeTarget, "target", "", "Target sandbox name (default: base sandbox)")
	sandboxMergeCmd.Flags().BoolVar(&sandboxMergeDryRun, "dry-run", false, "Preview the merge without contacting the server")

	sandboxDeleteCmd.Flags().BoolVar(&sandboxDeleteYes, "yes", false, "Skip confirmation prompt")
	sandboxDeleteCmd.Flags().BoolVar(&sandboxDeleteDryRun, "dry-run", false, "Preview the delete without contacting the server")
}
