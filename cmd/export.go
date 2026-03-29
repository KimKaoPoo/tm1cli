package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"tm1cli/internal/client"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

const mdxCellPageSize = 10000

var (
	exportView     string
	exportMDX      string
	exportOut      string
	exportNoHeader bool
)

var exportCmd = &cobra.Command{
	Use:   "export [cube]",
	Short: "Export cube data to screen or file",
	Long: `Export cube data to screen or file.

Equivalent to: File → Export in TM1 Architect
               or Export View in PAW
REST API:      POST /Cubes('name')/Views('view')/tm1.Execute
               POST /ExecuteMDX

Use --view with a cube name to export a saved view.
Use --mdx to export using an MDX query (cube name is optional).`,
	Example: `  # View-based export
  tm1cli export "Sales" --view "Default"
  tm1cli export "Sales" --view "Default" -o report.csv
  tm1cli export "Sales" --view "Default" -o report.json

  # MDX-based export
  tm1cli export --mdx "SELECT {[Period].[Jan]} ON COLUMNS, {[Measure].[Revenue]} ON ROWS FROM [Sales]"
  tm1cli export --mdx "SELECT ... FROM [Sales]" -o report.csv`,
	Args: cobra.RangeArgs(0, 1),
	RunE: runExport,
}

func runExport(cmd *cobra.Command, args []string) error {
	cubeName := ""
	if len(args) > 0 {
		cubeName = args[0]
	}

	if exportView == "" && exportMDX == "" {
		if cubeName != "" {
			return fmt.Errorf("Specify --view or --mdx. Example: tm1cli export \"%s\" --view \"Default\"", cubeName)
		}
		return fmt.Errorf("Specify --view or --mdx. Example: tm1cli export \"MyCube\" --view \"Default\"")
	}

	if exportView != "" && exportMDX != "" {
		return fmt.Errorf("Specify --view or --mdx, not both.")
	}

	if exportView != "" && cubeName == "" {
		return fmt.Errorf("Cube name is required with --view. Example: tm1cli export \"MyCube\" --view \"Default\"")
	}

	// Validate file extension before doing any network calls
	if exportOut != "" {
		ext := strings.ToLower(exportOut)
		if strings.HasSuffix(ext, ".xlsx") {
			return fmt.Errorf("XLSX export is not yet implemented (coming in v0.2.0).")
		}
		if !strings.HasSuffix(ext, ".csv") && !strings.HasSuffix(ext, ".json") {
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

	if exportMDX != "" {
		return runMDXExport(cl, jsonMode)
	}

	return runViewExport(cl, cubeName, jsonMode)
}

func runViewExport(cl *client.Client, cubeName string, jsonMode bool) error {
	endpoint := fmt.Sprintf("Cubes('%s')/Views('%s')/tm1.Execute?$expand=Axes($expand=Tuples($expand=Members($select=Name))),Cells($select=Value,Ordinal)", url.PathEscape(cubeName), url.PathEscape(exportView))

	data, err := cl.Post(endpoint, map[string]interface{}{})
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var resp model.CellsetResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse cellset response.", jsonMode)
		return errSilent
	}

	return outputCellset(resp, jsonMode)
}

func runMDXExport(cl *client.Client, jsonMode bool) error {
	// Step 1: Execute MDX — expand axes only, fetch cells separately for pagination
	endpoint := "ExecuteMDX?$expand=Axes($expand=Tuples($expand=Members($select=Name)))"
	payload := map[string]interface{}{"MDX": exportMDX}

	data, err := cl.Post(endpoint, payload)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var resp model.CellsetResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse cellset response.", jsonMode)
		return errSilent
	}

	if resp.ID == "" {
		output.PrintError("Server did not return a cellset ID.", jsonMode)
		return errSilent
	}

	// Step 2: Clean up cellset when done (runs even on error)
	defer func() {
		_ = cl.Delete(fmt.Sprintf("Cellsets('%s')", resp.ID))
	}()

	// Step 3: Calculate expected total cells for progress display
	totalCells := 1
	for _, axis := range resp.Axes {
		totalCells *= len(axis.Tuples)
	}

	// Step 4: Fetch cells in pages
	var allCells []model.CellsetCell
	for skip := 0; ; skip += mdxCellPageSize {
		cellsEndpoint := fmt.Sprintf("Cellsets('%s')/Cells?$select=Value,Ordinal&$top=%d&$skip=%d",
			resp.ID, mdxCellPageSize, skip)

		cellData, err := cl.Get(cellsEndpoint)
		if err != nil {
			output.PrintError(err.Error(), jsonMode)
			return errSilent
		}

		var cellResp struct {
			Value []model.CellsetCell `json:"value"`
		}
		if err := json.Unmarshal(cellData, &cellResp); err != nil {
			output.PrintError("Cannot parse cells response.", jsonMode)
			return errSilent
		}

		allCells = append(allCells, cellResp.Value...)

		// Show progress for large exports (more than one page)
		if totalCells > mdxCellPageSize {
			fetched := len(allCells)
			if fetched > totalCells {
				fetched = totalCells
			}
			fmt.Fprintf(os.Stderr, "Fetching cells: %d / %d\n", fetched, totalCells)
		}

		if len(cellResp.Value) < mdxCellPageSize {
			break
		}
	}

	resp.Cells = allCells
	return outputCellset(resp, jsonMode)
}

func outputCellset(resp model.CellsetResponse, jsonMode bool) error {
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
		record := make(map[string]interface{}, len(tuple.Members)+numCols)
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

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		f.Close()
		return fmt.Errorf("Cannot encode JSON: %s", err.Error())
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("Cannot write file: %s", err.Error())
	}
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
	exportCmd.Flags().StringVar(&exportMDX, "mdx", "", "MDX query string")
	exportCmd.Flags().StringVarP(&exportOut, "out", "o", "", "Output file path (.csv, .json)")
	exportCmd.Flags().BoolVar(&exportNoHeader, "no-header", false, "Exclude header row from CSV output")
}
