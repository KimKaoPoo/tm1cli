package cmd

import (
	"encoding/csv"
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
	exportView     string
	exportMDX      string
	exportOut      string
	exportNoHeader bool
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

	// Validate file extension before doing any network calls
	if exportOut != "" {
		ext := strings.ToLower(exportOut)
		if strings.HasSuffix(ext, ".xlsx") {
			return fmt.Errorf("XLSX export is not yet implemented (coming in v0.2.0).")
		}
		if strings.HasSuffix(ext, ".json") {
			return fmt.Errorf("JSON file export is not yet implemented (coming in v0.1.1).")
		}
		if !strings.HasSuffix(ext, ".csv") {
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

	// CSV file output
	if strings.HasSuffix(strings.ToLower(exportOut), ".csv") {
		return writeCSV(resp, exportOut, exportNoHeader)
	}

	if jsonMode {
		output.PrintJSON(resp)
		return nil
	}

	printCellsetTable(resp)
	return nil
}

// buildCellsetRows converts a CellsetResponse into headers and row data.
// Returns nil, nil if the response has fewer than 2 axes or 0 column tuples.
func buildCellsetRows(resp model.CellsetResponse) ([]string, [][]string) {
	if len(resp.Axes) < 2 {
		return nil, nil
	}

	colAxis := resp.Axes[0]
	rowAxis := resp.Axes[1]

	numCols := len(colAxis.Tuples)
	if numCols == 0 {
		return nil, nil
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

	// Headers
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

	return headers, rows
}

func printCellsetTable(resp model.CellsetResponse) {
	headers, rows := buildCellsetRows(resp)
	if headers == nil {
		fmt.Println("No data returned.")
		return
	}
	output.PrintTable(headers, rows)
}

func writeCSV(resp model.CellsetResponse, filePath string, noHeader bool) error {
	headers, rows := buildCellsetRows(resp)
	if headers == nil {
		fmt.Fprintln(os.Stderr, "No data to export.")
		return nil
	}

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("Cannot create file: %s", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)

	if !noHeader {
		if err := w.Write(headers); err != nil {
			return fmt.Errorf("Cannot write CSV header: %s", err)
		}
	}

	for _, row := range rows {
		if err := w.Write(row); err != nil {
			return fmt.Errorf("Cannot write CSV row: %s", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return fmt.Errorf("Cannot write CSV: %s", err)
	}

	fmt.Fprintf(os.Stderr, "Exported %d rows to %s\n", len(rows), filePath)
	return nil
}

func init() {
	rootCmd.AddCommand(exportCmd)
	exportCmd.Flags().StringVar(&exportView, "view", "", "Saved view name")
	exportCmd.Flags().StringVar(&exportMDX, "mdx", "", "MDX query string (v0.2.0)")
	exportCmd.Flags().StringVarP(&exportOut, "out", "o", "", "Output file path (.csv, .json)")
	exportCmd.Flags().BoolVar(&exportNoHeader, "no-header", false, "Exclude header row from CSV output")
}
