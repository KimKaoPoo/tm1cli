package cmd

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var processCmd = &cobra.Command{
	Use:   "process",
	Short: "Manage and run TI processes",
	Long: `Manage and run TI (TurboIntegrator) processes.

Equivalent to: TI Process Editor in TM1 Architect
               or running processes in PAW Administration`,
}

// --- process list ---

var (
	procListFilter     string
	procListLimit      int
	procListAll        bool
	procListShowSystem bool
	procListCount      bool
)

var processListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all TI processes",
	Long: `List all TI processes on the server.

Equivalent to: Server Explorer → Processes in TM1 Architect
REST API:      GET /Processes

System processes (starting with }) are hidden by default.`,
	Example: `  tm1cli process list
  tm1cli process list --filter "load"
  tm1cli process list --all
  tm1cli process list --show-system
  tm1cli process list --count`,
	RunE: runProcessList,
}

func runProcessList(cmd *cobra.Command, args []string) error {
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

	showSystem := getShowSystem(cfg, procListShowSystem)
	limit := getLimit(cfg, procListLimit, procListAll)

	endpoint := "Processes?$select=Name"

	// Try server-side filter
	filterFallback := false
	if procListFilter != "" {
		safeFilter := strings.ReplaceAll(procListFilter, "'", "''")
		filterEndpoint := endpoint + fmt.Sprintf("&$filter=contains(tolower(Name),tolower('%s'))", safeFilter)
		data, err := cl.Get(filterEndpoint)
		if err == nil {
			var resp model.ProcessResponse
			if jsonErr := json.Unmarshal(data, &resp); jsonErr == nil {
				procs := filterSystemProcesses(resp.Value, showSystem)
				displayProcesses(procs, len(procs), limit, jsonMode)
				return nil
			}
		}
		filterFallback = true
		output.PrintWarning("Server-side filter not supported, filtering locally...")
	}

	fetchEndpoint := endpoint
	if limit > 0 && procListFilter == "" {
		fetchEndpoint += fmt.Sprintf("&$top=%d", limit+500)
	}

	data, err := cl.Get(fetchEndpoint)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var resp model.ProcessResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	procs := filterSystemProcesses(resp.Value, showSystem)

	if procListFilter != "" && filterFallback {
		procs = filterProcessesByName(procs, procListFilter)
	}

	displayProcesses(procs, len(procs), limit, jsonMode)
	return nil
}

func filterSystemProcesses(procs []model.Process, showSystem bool) []model.Process {
	if showSystem {
		return procs
	}
	var filtered []model.Process
	for _, p := range procs {
		if !strings.HasPrefix(p.Name, "}") {
			filtered = append(filtered, p)
		}
	}
	return filtered
}

func filterProcessesByName(procs []model.Process, filter string) []model.Process {
	lower := strings.ToLower(filter)
	var filtered []model.Process
	for _, p := range procs {
		if strings.Contains(strings.ToLower(p.Name), lower) {
			filtered = append(filtered, p)
		}
	}
	return filtered
}

func displayProcesses(procs []model.Process, total int, limit int, jsonMode bool) {
	if procListCount {
		if jsonMode {
			output.PrintJSON(map[string]int{"count": total})
		} else {
			fmt.Printf("%d processes\n", total)
		}
		return
	}

	shown := procs
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}

	if jsonMode {
		output.PrintJSON(shown)
		return
	}

	headers := []string{"NAME"}
	rows := make([][]string, len(shown))
	for i, p := range shown {
		rows[i] = []string{p.Name}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(shown), total)
}

// --- process run ---

var procRunParams []string

var processRunCmd = &cobra.Command{
	Use:   "run <name>",
	Short: "Execute a TI process",
	Long: `Execute a TI process on the TM1 server.

Equivalent to: Right-click → Run in TM1 Architect
               or Run Process in PAW Administration
REST API:      POST /Processes('name')/tm1.Execute

Parameters map to the Parameters tab in the TI editor
(the pSource, pYear, etc. you define in the process).`,
	Example: `  tm1cli process run "LoadData"
  tm1cli process run "LoadData" --param pSource=file.csv --param pYear=2024
  tm1cli process run "RunReport" --server production
  tm1cli process run "LoadData" --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runProcessRun,
}

// parseProcessParam splits a "Key=Value" string on the first '=' sign.
// Values may contain additional '=' characters.
func parseProcessParam(s string) (string, string, error) {
	idx := strings.Index(s, "=")
	if idx < 0 {
		return "", "", fmt.Errorf("Invalid parameter format '%s'. Use Key=Value.", s)
	}
	return s[:idx], s[idx+1:], nil
}

func runProcessRun(cmd *cobra.Command, args []string) error {
	processName := args[0]

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

	// Build request body
	var body interface{}
	if len(procRunParams) > 0 {
		params := make([]model.ProcessParameter, 0, len(procRunParams))
		for _, p := range procRunParams {
			name, value, err := parseProcessParam(p)
			if err != nil {
				output.PrintError(err.Error(), jsonMode)
				return errSilent
			}
			params = append(params, model.ProcessParameter{
				Name:  name,
				Value: value,
			})
		}
		body = model.ProcessExecuteBody{Parameters: params}
	} else {
		body = map[string]interface{}{}
	}

	endpoint := fmt.Sprintf("Processes('%s')/tm1.Execute", url.PathEscape(processName))

	start := time.Now()
	_, err = cl.Post(endpoint, body)
	elapsed := time.Since(start)

	result := model.ProcessRunResult{
		Process:    processName,
		DurationMs: elapsed.Milliseconds(),
	}

	if err != nil {
		result.Status = "error"
		result.Message = err.Error()

		if jsonMode {
			output.PrintJSON(result)
		} else {
			fmt.Printf("Process '%s' failed.\n", processName)
			fmt.Printf("  Status:  Error\n")
			fmt.Printf("  Message: %s\n", err.Error())
		}
		return errSilent
	}

	result.Status = "completed"
	result.Message = fmt.Sprintf("Process '%s' executed successfully.", processName)

	if jsonMode {
		output.PrintJSON(result)
	} else {
		fmt.Printf("Process '%s' executed successfully.\n", processName)
		fmt.Printf("  Status:   Completed\n")
		fmt.Printf("  Duration: %.1fs\n", elapsed.Seconds())
	}
	return nil
}

// --- process dump ---

var procDumpOut string

var processDumpCmd = &cobra.Command{
	Use:   "dump <name>",
	Short: "Export a TI process definition to file",
	Long: `Export a TI process definition to a JSON or YAML file.

Equivalent to: Right-click → Export in TM1 Architect
REST API:      GET /Processes('name')

Format is detected from file extension (.json, .yaml, .yml).
Without -o flag, prints JSON to stdout.`,
	Example: `  tm1cli process dump "LoadData" -o loaddata.yaml
  tm1cli process dump "LoadData" -o loaddata.json
  tm1cli process dump "LoadData"`,
	Args: cobra.ExactArgs(1),
	RunE: runProcessDump,
}

func runProcessDump(cmd *cobra.Command, args []string) error {
	processName := args[0]

	var ext string
	if procDumpOut != "" {
		ext = strings.ToLower(filepath.Ext(procDumpOut))
		if ext != ".json" && ext != ".yaml" && ext != ".yml" {
			return fmt.Errorf("Unsupported format %q. Use .json, .yaml, or .yml.", ext)
		}
	}

	cfg, err := loadConfig()
	if err != nil {
		output.PrintError(err.Error(), isJSONOutput(nil))
		return errSilent
	}

	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), false)
		return errSilent
	}

	endpoint := fmt.Sprintf("Processes('%s')?$expand=Parameters,Variables", url.PathEscape(processName))
	data, err := cl.Get(endpoint)
	if err != nil {
		output.PrintError(err.Error(), false)
		return errSilent
	}

	var detail model.ProcessDetail
	if err := json.Unmarshal(data, &detail); err != nil {
		output.PrintError("Cannot parse process definition from server response.", false)
		return errSilent
	}

	if procDumpOut == "" {
		out, err := json.MarshalIndent(detail, "", "  ")
		if err != nil {
			output.PrintError("Cannot serialize process definition.", false)
			return errSilent
		}
		fmt.Println(string(out))
		return nil
	}

	switch ext {
	case ".json":
		if err := writeJSONFile(procDumpOut, detail); err != nil {
			output.PrintError(err.Error(), false)
			return errSilent
		}
	case ".yaml", ".yml":
		out, err := yaml.Marshal(detail)
		if err != nil {
			output.PrintError("Cannot serialize process definition.", false)
			return errSilent
		}
		if err := os.WriteFile(procDumpOut, out, 0644); err != nil {
			output.PrintError(fmt.Sprintf("Cannot write file: %s", err.Error()), false)
			return errSilent
		}
	}

	fmt.Fprintf(os.Stderr, "Exported process '%s' to %s\n", processName, procDumpOut)
	return nil
}

// --- process load ---

var (
	procLoadFile       string
	procLoadCreateOnly bool
	procLoadUpdateOnly bool
)

var processLoadCmd = &cobra.Command{
	Use:   "load <name>",
	Short: "Import a TI process from file",
	Long: `Import a TI process definition from a JSON or YAML file.

Equivalent to: Import in TM1 Architect
REST API:      PATCH /Processes('name') or POST /Processes

Format is detected from file extension (.json, .yaml, .yml).
By default, tries to update an existing process (PATCH).
If the process does not exist, creates it (POST).`,
	Example: `  tm1cli process load "LoadData" -f loaddata.yaml
  tm1cli process load "LoadData" -f loaddata.json
  tm1cli process load "NewProcess" -f process.yaml --create-only
  tm1cli process load "LoadData" -f process.yaml --update-only`,
	Args: cobra.ExactArgs(1),
	RunE: runProcessLoad,
}

func runProcessLoad(cmd *cobra.Command, args []string) error {
	processName := args[0]

	if procLoadFile == "" {
		return fmt.Errorf("--file (-f) is required")
	}

	if procLoadCreateOnly && procLoadUpdateOnly {
		return fmt.Errorf("--create-only and --update-only are mutually exclusive")
	}

	ext := strings.ToLower(filepath.Ext(procLoadFile))
	if ext != ".json" && ext != ".yaml" && ext != ".yml" {
		return fmt.Errorf("Unsupported format %q. Use .json, .yaml, or .yml.", ext)
	}

	cfg, err := loadConfig()
	if err != nil {
		output.PrintError(err.Error(), isJSONOutput(nil))
		return errSilent
	}

	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), false)
		return errSilent
	}

	fileData, err := os.ReadFile(procLoadFile)
	if err != nil {
		output.PrintError(fmt.Sprintf("Cannot read file: %s", err.Error()), false)
		return errSilent
	}

	var detail model.ProcessDetail
	switch ext {
	case ".json":
		if err := json.Unmarshal(fileData, &detail); err != nil {
			output.PrintError("Cannot parse JSON file.", false)
			return errSilent
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(fileData, &detail); err != nil {
			output.PrintError("Cannot parse YAML file.", false)
			return errSilent
		}
	}

	// CLI argument overrides the name stored in the file
	detail.Name = processName

	if procLoadCreateOnly {
		_, err := cl.Post("Processes", detail)
		if err != nil {
			output.PrintError(err.Error(), false)
			return errSilent
		}
		fmt.Printf("Process '%s' created successfully.\n", processName)
		return nil
	}

	patchEndpoint := fmt.Sprintf("Processes('%s')", url.PathEscape(processName))

	if procLoadUpdateOnly {
		_, status, err := cl.Patch(patchEndpoint, detail)
		if err != nil {
			if status == 404 {
				output.PrintError(fmt.Sprintf("Process '%s' not found. Cannot update.", processName), false)
			} else {
				output.PrintError(err.Error(), false)
			}
			return errSilent
		}
		fmt.Printf("Process '%s' updated successfully.\n", processName)
		return nil
	}

	// Default: try PATCH, fall back to POST only on 404
	_, status, patchErr := cl.Patch(patchEndpoint, detail)
	if patchErr == nil {
		fmt.Printf("Process '%s' updated successfully.\n", processName)
		return nil
	}

	if status != 404 {
		output.PrintError(patchErr.Error(), false)
		return errSilent
	}

	// Process not found — create it
	_, postErr := cl.Post("Processes", detail)
	if postErr != nil {
		output.PrintError(postErr.Error(), false)
		return errSilent
	}
	fmt.Printf("Process '%s' created successfully.\n", processName)
	return nil
}

func init() {
	rootCmd.AddCommand(processCmd)
	processCmd.AddCommand(processListCmd)
	processCmd.AddCommand(processRunCmd)

	processListCmd.Flags().StringVar(&procListFilter, "filter", "", "Filter by name (case-insensitive, partial match)")
	processListCmd.Flags().IntVar(&procListLimit, "limit", 0, "Max results to show (default from settings)")
	processListCmd.Flags().BoolVar(&procListAll, "all", false, "Show all results, no limit")
	processListCmd.Flags().BoolVar(&procListShowSystem, "show-system", false, "Include system processes (names starting with })")
	processListCmd.Flags().BoolVar(&procListCount, "count", false, "Show count only")

	processRunCmd.Flags().StringArrayVar(&procRunParams, "param", nil, "Process parameter as Key=Value (repeatable)")

	processCmd.AddCommand(processDumpCmd)
	processCmd.AddCommand(processLoadCmd)

	processDumpCmd.Flags().StringVarP(&procDumpOut, "out", "o", "", "Output file path (.json, .yaml, .yml)")

	processLoadCmd.Flags().StringVarP(&procLoadFile, "file", "f", "", "Input file path (.json, .yaml, .yml)")
	processLoadCmd.Flags().BoolVar(&procLoadCreateOnly, "create-only", false, "Only create new process, fail if exists")
	processLoadCmd.Flags().BoolVar(&procLoadUpdateOnly, "update-only", false, "Only update existing process, fail if not found")
}
