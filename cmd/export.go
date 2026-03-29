package cmd

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

var (
	exportView string
	exportMDX  string
	exportOut  string
)

var exportCmd = &cobra.Command{
	Use:   "export <cube>",
	Short: "Export cube data to screen or file",
	Long: `Export cube data to screen or file.

Equivalent to: File → Export in TM1 Architect
               or Export View in PAW
REST API:      GET /Cubes('name')/Views('view')/tm1.Execute
               POST /ExecuteMDX`,
	Example: `  tm1cli export "Sales" --view "Default"
  tm1cli export "Sales" --view "Default" -o report.csv
  tm1cli export "Sales" --view "Default" --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runExport,
}

func runExport(cmd *cobra.Command, args []string) error {
	cubeName := args[0]

	if exportView == "" && exportMDX == "" {
		return fmt.Errorf("Specify --view or --mdx. Example: tm1cli export \"%s\" --view \"Default\"", cubeName)
	}

	// TODO: Phase 2 — MDX export
	if exportMDX != "" {
		return fmt.Errorf("MDX export is not yet implemented (coming in v0.2.0). Use --view instead.")
	}

	// Validate file output format early (before API call)
	if exportOut != "" {
		ext := strings.ToLower(exportOut)
		if strings.HasSuffix(ext, ".xlsx") {
			return fmt.Errorf("XLSX export is not yet implemented (coming in v0.2.0).")
		}
		if strings.HasSuffix(ext, ".csv") {
			return fmt.Errorf("CSV export is not yet implemented (coming in v0.1.1).")
		}
		if !strings.HasSuffix(ext, ".json") {
			return fmt.Errorf("Unsupported file format. Supported: .csv, .json, .xlsx")
		}
	}

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

	endpoint := fmt.Sprintf("Cubes('%s')/Views('%s')/tm1.Execute?$expand=Axes($expand=Tuples($expand=Members($select=Name))),Cells($select=Value,Ordinal)", url.PathEscape(cubeName), url.PathEscape(exportView))

	data, err := cl.Get(endpoint)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var resp model.CellsetResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse cellset response.", jsonMode)
		return errSilent
	}

	// JSON file output
	if strings.HasSuffix(strings.ToLower(exportOut), ".json") {
		records := cellsetToRecords(resp)
		if err := writeJSONFile(exportOut, records); err != nil {
			output.PrintError(err.Error(), jsonMode)
			return errSilent
		}
		fmt.Fprintf(os.Stderr, "Wrote %d records to %s\n", len(records), exportOut)
		return nil
	}

	if jsonMode {
		output.PrintJSON(resp)
		return nil
	}

	printCellsetTable(resp)
	return nil
}

// cellsetToRecords converts a CellsetResponse into a flat array of record maps.
// Row dimension members become DIM1, DIM2, etc. Column headers become value keys.
func cellsetToRecords(resp model.CellsetResponse) []map[string]interface{} {
	if len(resp.Axes) < 2 {
		return []map[string]interface{}{}
	}

	colAxis := resp.Axes[0]
	rowAxis := resp.Axes[1]

	numCols := len(colAxis.Tuples)
	if numCols == 0 {
		return []map[string]interface{}{}
	}

	// Build column header names
	colHeaders := make([]string, numCols)
	for i, tuple := range colAxis.Tuples {
		names := make([]string, len(tuple.Members))
		for j, m := range tuple.Members {
			names[j] = m.Name
		}
		colHeaders[i] = strings.Join(names, " / ")
	}

	// Index cells by ordinal
	cellsByOrdinal := make(map[int]interface{}, len(resp.Cells))
	for _, cell := range resp.Cells {
		cellsByOrdinal[cell.Ordinal] = cell.Value
	}

	// Build records
	records := make([]map[string]interface{}, 0, len(rowAxis.Tuples))
	for r, tuple := range rowAxis.Tuples {
		record := make(map[string]interface{})
		for d, m := range tuple.Members {
			record[fmt.Sprintf("DIM%d", d+1)] = m.Name
		}
		for c := 0; c < numCols; c++ {
			ordinal := r*numCols + c
			if v, ok := cellsByOrdinal[ordinal]; ok {
				record[colHeaders[c]] = v
			} else {
				record[colHeaders[c]] = nil
			}
		}
		records = append(records, record)
	}

	return records
}

func writeJSONFile(filePath string, data interface{}) error {
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("Cannot write file: %s", err.Error())
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		return fmt.Errorf("Cannot encode JSON: %s", err.Error())
	}
	return nil
}

func printCellsetTable(resp model.CellsetResponse) {
	if len(resp.Axes) < 2 {
		fmt.Println("No data returned.")
		return
	}

	colAxis := resp.Axes[0]
	rowAxis := resp.Axes[1]

	numCols := len(colAxis.Tuples)
	if numCols == 0 {
		fmt.Println("No data returned.")
		return
	}

	// Build column headers
	colHeaders := make([]string, numCols)
	for i, tuple := range colAxis.Tuples {
		names := make([]string, len(tuple.Members))
		for j, m := range tuple.Members {
			names[j] = m.Name
		}
		colHeaders[i] = strings.Join(names, " / ")
	}

	// Build row headers count
	rowMemberCount := 0
	if len(rowAxis.Tuples) > 0 {
		rowMemberCount = len(rowAxis.Tuples[0].Members)
	}

	// Table headers
	headers := make([]string, 0, rowMemberCount+numCols)
	for i := 0; i < rowMemberCount; i++ {
		headers = append(headers, fmt.Sprintf("DIM%d", i+1))
	}
	headers = append(headers, colHeaders...)

	// Index cells by ordinal for O(1) lookup
	cellsByOrdinal := make(map[int]interface{}, len(resp.Cells))
	for _, cell := range resp.Cells {
		cellsByOrdinal[cell.Ordinal] = cell.Value
	}

	// Build rows
	rows := make([][]string, len(rowAxis.Tuples))
	for r, tuple := range rowAxis.Tuples {
		row := make([]string, 0, rowMemberCount+numCols)
		for _, m := range tuple.Members {
			row = append(row, m.Name)
		}
		for c := 0; c < numCols; c++ {
			ordinal := r*numCols + c
			val := ""
			if v, ok := cellsByOrdinal[ordinal]; ok && v != nil {
				val = fmt.Sprintf("%v", v)
			}
			row = append(row, val)
		}
		rows[r] = row
	}

	output.PrintTable(headers, rows)
}

func init() {
	rootCmd.AddCommand(exportCmd)
	exportCmd.Flags().StringVar(&exportView, "view", "", "Saved view name")
	exportCmd.Flags().StringVar(&exportMDX, "mdx", "", "MDX query string (v0.2.0)")
	exportCmd.Flags().StringVarP(&exportOut, "out", "o", "", "Output file path (.csv, .json)")
}
