package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"tm1cli/internal/client"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
	"github.com/xuri/excelize/v2"
)

const mdxCellPageSize = 10000

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
REST API:      POST /Cubes('name')/Views('view')/tm1.Execute
               POST /ExecuteMDX`,
	Example: `  # View-based export
  tm1cli export "Sales" --view "Default"
  tm1cli export "Sales" --view "Default" -o report.csv
  tm1cli export "Sales" --view "Default" -o report.xlsx

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
		if !strings.HasSuffix(ext, ".csv") && !strings.HasSuffix(ext, ".json") && !strings.HasSuffix(ext, ".xlsx") {
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
	endpoint := fmt.Sprintf("Cubes('%s')/Views('%s')/tm1.Execute?$expand=Axes($expand=Tuples($expand=Members($select=Name,UniqueName))),Cells($select=Value,Ordinal)", url.PathEscape(cubeName), url.PathEscape(exportView))

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
	endpoint := "ExecuteMDX?$expand=Axes($expand=Tuples($expand=Members($select=Name,UniqueName)))"
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
	allCells := make([]model.CellsetCell, 0, totalCells)
	for skip := 0; ; skip += mdxCellPageSize {
		cellsEndpoint := fmt.Sprintf("Cellsets('%s')/Cells?$select=Value,Ordinal&$top=%d&$skip=%d",
			resp.ID, mdxCellPageSize, skip)

		cellData, err := cl.Get(cellsEndpoint)
		if err != nil {
			output.PrintError(err.Error(), jsonMode)
			return errSilent
		}

		var cellResp model.CellsCollectionResponse
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
		if err := writeCSV(resp, exportOut, exportNoHeader); err != nil {
			output.PrintError(err.Error(), jsonMode)
			return errSilent
		}
		return nil
	}

	// XLSX file output
	if strings.HasSuffix(strings.ToLower(exportOut), ".xlsx") {
		if err := writeXLSX(resp, exportOut, exportNoHeader); err != nil {
			output.PrintError(err.Error(), jsonMode)
			return errSilent
		}
		return nil
	}

	if jsonMode {
		output.PrintJSON(resp)
		return nil
	}

	printCellsetTable(resp)
	return nil
}

// cellsetLayout is the canonical flattened representation shared by all writers.
// Headers[i] and RowMembers[r][i] (for i<NumRowDims) or RowCells[r][i-NumRowDims]
// (for i>=NumRowDims) are aligned positionally.
type cellsetLayout struct {
	Headers     []string
	RowMembers  [][]string
	RowCells    [][]interface{}
	NumRowDims  int
	ConstantCol []bool

	Scalar      bool
	ScalarValue interface{}
	SingleAxis  bool
}

// parseBracketSegments splits "[A].[B].[C]" into ["A","B","C"], honoring the
// TM1 MDX escape where "]]" inside brackets represents a literal "]".
// Returns nil on malformed input.
func parseBracketSegments(s string) []string {
	var out []string
	i := 0
	for i < len(s) {
		if s[i] != '[' {
			return nil
		}
		i++
		var b strings.Builder
		for {
			if i >= len(s) {
				return nil
			}
			if s[i] == ']' {
				if i+1 < len(s) && s[i+1] == ']' {
					b.WriteByte(']')
					i += 2
					continue
				}
				break
			}
			b.WriteByte(s[i])
			i++
		}
		out = append(out, b.String())
		i++
		if i < len(s) {
			if s[i] != '.' {
				return nil
			}
			i++
		}
	}
	return out
}

// deriveDimensionLabel returns a human-readable bracketed label from a
// UniqueName. Returns "" if the input is unparseable; the caller then
// falls back to DIM{N}.
//
//	"[Period].[Period].[Jan]" -> "[Period]"
//	"[Period].[FY].[2024]"    -> "[Period:FY]"
//	"[Version].[Actual]"      -> "[Version]"
func deriveDimensionLabel(uniqueName string) string {
	segs := parseBracketSegments(uniqueName)
	switch len(segs) {
	case 0, 1:
		return ""
	case 2:
		return "[" + segs[0] + "]"
	default:
		if segs[0] == segs[1] {
			return "[" + segs[0] + "]"
		}
		return "[" + segs[0] + ":" + segs[1] + "]"
	}
}

// sortedAxes returns resp.Axes sorted by Ordinal ascending. Duplicate ordinals
// are dropped (first occurrence kept) and a warning is emitted.
func sortedAxes(resp model.CellsetResponse) []model.CellsetAxis {
	axes := make([]model.CellsetAxis, len(resp.Axes))
	copy(axes, resp.Axes)
	sort.SliceStable(axes, func(i, j int) bool { return axes[i].Ordinal < axes[j].Ordinal })

	seen := make(map[int]bool, len(axes))
	out := axes[:0]
	for _, a := range axes {
		if seen[a.Ordinal] {
			output.PrintWarning(fmt.Sprintf("duplicate axis ordinal %d; dropping duplicate", a.Ordinal))
			continue
		}
		seen[a.Ordinal] = true
		out = append(out, a)
	}
	return out
}

// disambiguateLabels returns a copy where duplicate labels gain a "(N)" suffix
// for their second, third, etc. occurrences. Preserves positional alignment.
func disambiguateLabels(labels []string) []string {
	counts := make(map[string]int, len(labels))
	out := make([]string, len(labels))
	for i, lbl := range labels {
		counts[lbl]++
		if counts[lbl] == 1 {
			out[i] = lbl
		} else {
			out[i] = fmt.Sprintf("%s(%d)", lbl, counts[lbl])
		}
	}
	return out
}

// buildColHeaders joins multi-member column tuples with " / ".
func buildColHeaders(colAxis model.CellsetAxis) []string {
	out := make([]string, len(colAxis.Tuples))
	for i, t := range colAxis.Tuples {
		names := make([]string, len(t.Members))
		for j, m := range t.Members {
			names[j] = m.Name
		}
		out[i] = strings.Join(names, " / ")
	}
	return out
}

// buildRowLabelsForAxis derives row-dim labels for a single axis, falling
// back to DIM{N} when UniqueName is absent or unparseable. Global
// disambiguation (via disambiguateLabels) happens after all axes' labels
// are concatenated.
func buildRowLabelsForAxis(a model.CellsetAxis, memberCount int) []string {
	out := make([]string, memberCount)
	var first model.CellsetTuple
	if len(a.Tuples) > 0 {
		first = a.Tuples[0]
	}
	for i := 0; i < memberCount; i++ {
		lbl := ""
		if i < len(first.Members) {
			lbl = deriveDimensionLabel(first.Members[i].UniqueName)
		}
		if lbl == "" {
			lbl = fmt.Sprintf("DIM%d", i+1)
		}
		out[i] = lbl
	}
	return out
}

// formatCellValue stringifies a cell value; nil becomes "" (no "<nil>").
func formatCellValue(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

// buildCellsetLayout produces a canonical flattened representation. Returns
// nil when the response has no meaningful rendering (preserves the existing
// "No data returned." behavior for empty 2-axis cases).
func buildCellsetLayout(resp model.CellsetResponse) *cellsetLayout {
	axes := sortedAxes(resp)

	if len(axes) == 0 {
		var v interface{}
		if len(resp.Cells) > 0 {
			v = resp.Cells[0].Value
		}
		return &cellsetLayout{
			Scalar:      true,
			ScalarValue: v,
			Headers:     []string{"Value"},
		}
	}

	cells := make(map[int]interface{}, len(resp.Cells))
	for _, c := range resp.Cells {
		cells[c.Ordinal] = c.Value
	}

	if len(axes) == 1 {
		colAxis := axes[0]
		numCols := len(colAxis.Tuples)
		if numCols == 0 {
			return nil
		}
		headers := disambiguateLabels(buildColHeaders(colAxis))
		rowCells := make([]interface{}, numCols)
		for c := 0; c < numCols; c++ {
			rowCells[c] = cells[c]
		}
		return &cellsetLayout{
			SingleAxis: true,
			Headers:    headers,
			RowMembers: [][]string{{}},
			RowCells:   [][]interface{}{rowCells},
		}
	}

	colAxis := axes[0]
	rowAxes := axes[1:]
	numCols := len(colAxis.Tuples)
	if numCols == 0 {
		return nil
	}
	for _, a := range rowAxes {
		if len(a.Tuples) == 0 {
			return nil
		}
	}

	var rowLabels []string
	axisSizes := make([]int, len(rowAxes))
	for k, a := range rowAxes {
		axisSizes[k] = len(a.Tuples)
		mc := len(a.Tuples[0].Members)
		rowLabels = append(rowLabels, buildRowLabelsForAxis(a, mc)...)
	}
	colHeaders := buildColHeaders(colAxis)

	combined := make([]string, 0, len(rowLabels)+len(colHeaders))
	combined = append(combined, rowLabels...)
	combined = append(combined, colHeaders...)
	all := disambiguateLabels(combined)
	numRowDims := len(rowLabels)
	rowLabels = all[:numRowDims]
	colHeaders = all[numRowDims:]

	totalRows := 1
	for _, sz := range axisSizes {
		totalRows *= sz
	}

	rowMembers := make([][]string, totalRows)
	rowCells := make([][]interface{}, totalRows)
	for r := 0; r < totalRows; r++ {
		mem := make([]string, 0, len(rowLabels))
		rem := r
		ord := 0
		stride := numCols
		for k, a := range rowAxes {
			idx := rem % axisSizes[k]
			rem /= axisSizes[k]
			ord += idx * stride
			stride *= axisSizes[k]
			for _, m := range a.Tuples[idx].Members {
				mem = append(mem, m.Name)
			}
		}
		rowMembers[r] = mem

		rc := make([]interface{}, numCols)
		for c := 0; c < numCols; c++ {
			rc[c] = cells[ord+c]
		}
		rowCells[r] = rc
	}

	constantCols := make([]bool, len(rowLabels))
	if totalRows > 0 {
		for c := range rowLabels {
			allSame := true
			first := rowMembers[0][c]
			for r := 1; r < totalRows; r++ {
				if rowMembers[r][c] != first {
					allSame = false
					break
				}
			}
			constantCols[c] = allSame
		}
	}

	return &cellsetLayout{
		Headers:     all,
		RowMembers:  rowMembers,
		RowCells:    rowCells,
		NumRowDims:  numRowDims,
		ConstantCol: constantCols,
	}
}

// layoutToStringRows produces the string-only row form used by CSV.
func layoutToStringRows(l *cellsetLayout) ([]string, [][]string) {
	if l.Scalar {
		return []string{"Value"}, [][]string{{formatCellValue(l.ScalarValue)}}
	}
	rows := make([][]string, len(l.RowCells))
	for r := range l.RowCells {
		row := make([]string, 0, len(l.Headers))
		row = append(row, l.RowMembers[r]...)
		for _, v := range l.RowCells[r] {
			row = append(row, formatCellValue(v))
		}
		rows[r] = row
	}
	return l.Headers, rows
}

// layoutToTypedRows keeps cell values as interface{} for XLSX numeric typing.
// Row-dim and slicer values remain strings (member names are strings).
func layoutToTypedRows(l *cellsetLayout) ([]string, [][]interface{}) {
	if l.Scalar {
		return []string{"Value"}, [][]interface{}{{l.ScalarValue}}
	}
	rows := make([][]interface{}, len(l.RowCells))
	for r := range l.RowCells {
		row := make([]interface{}, 0, len(l.Headers))
		for _, m := range l.RowMembers[r] {
			row = append(row, m)
		}
		for _, v := range l.RowCells[r] {
			row = append(row, v)
		}
		rows[r] = row
	}
	return l.Headers, rows
}

// cellsetToRecords converts a CellsetResponse into a flat array of record
// maps for JSON-file output. Row-dim labels come from UniqueName when
// available (falling back to DIM{N}); title axes and multi-tuple higher
// axes are flattened into row-dim fields. Header collisions are
// disambiguated globally (e.g., duplicate "[Period]" -> "[Period]", "[Period](2)").
func cellsetToRecords(resp model.CellsetResponse) []map[string]interface{} {
	layout := buildCellsetLayout(resp)
	if layout == nil {
		return []map[string]interface{}{}
	}
	if layout.Scalar {
		return []map[string]interface{}{{"Value": layout.ScalarValue}}
	}

	records := make([]map[string]interface{}, 0, len(layout.RowCells))
	for r := range layout.RowCells {
		rec := make(map[string]interface{}, len(layout.Headers))
		for c := 0; c < layout.NumRowDims; c++ {
			rec[layout.Headers[c]] = layout.RowMembers[r][c]
		}
		valHeaders := layout.Headers[layout.NumRowDims:]
		for c, h := range valHeaders {
			rec[h] = layout.RowCells[r][c]
		}
		records = append(records, rec)
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

func printCellsetTable(resp model.CellsetResponse) {
	layout := buildCellsetLayout(resp)
	if layout == nil {
		fmt.Println("No data returned.")
		return
	}

	if layout.Scalar {
		fmt.Printf("Result: %s\n", formatCellValue(layout.ScalarValue))
		return
	}

	// Promote constant row-dim columns to slicer preamble (table mode only).
	kept := make([]int, 0, layout.NumRowDims)
	for c := 0; c < layout.NumRowDims; c++ {
		if layout.ConstantCol[c] && len(layout.RowMembers) > 0 {
			fmt.Printf("Slicer: %s = %s\n", layout.Headers[c], layout.RowMembers[0][c])
		} else {
			kept = append(kept, c)
		}
	}

	headers := make([]string, 0, len(kept)+len(layout.Headers)-layout.NumRowDims)
	for _, c := range kept {
		headers = append(headers, layout.Headers[c])
	}
	headers = append(headers, layout.Headers[layout.NumRowDims:]...)

	rows := make([][]string, len(layout.RowCells))
	for r := range layout.RowCells {
		row := make([]string, 0, len(headers))
		for _, c := range kept {
			row = append(row, layout.RowMembers[r][c])
		}
		for _, v := range layout.RowCells[r] {
			row = append(row, formatCellValue(v))
		}
		rows[r] = row
	}
	output.PrintTable(headers, rows)
}

func writeCSV(resp model.CellsetResponse, filePath string, noHeader bool) error {
	layout := buildCellsetLayout(resp)
	if layout == nil {
		fmt.Fprintln(os.Stderr, "No data to export.")
		return nil
	}

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("Cannot create file: %s", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	headers, rows := layoutToStringRows(layout)

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

func writeXLSX(resp model.CellsetResponse, filePath string, noHeader bool) error {
	layout := buildCellsetLayout(resp)
	if layout == nil {
		fmt.Fprintln(os.Stderr, "No data to export.")
		return nil
	}

	headers, rows := layoutToTypedRows(layout)

	f := excelize.NewFile()
	defer f.Close()
	sheet := "Sheet1"

	rowIdx := 1
	if !noHeader {
		for i, h := range headers {
			cell, _ := excelize.CoordinatesToCellName(i+1, rowIdx)
			f.SetCellValue(sheet, cell, h)
		}
		rowIdx++
	}
	for _, row := range rows {
		for i, v := range row {
			cell, _ := excelize.CoordinatesToCellName(i+1, rowIdx)
			if v != nil {
				f.SetCellValue(sheet, cell, v)
			}
		}
		rowIdx++
	}

	if err := f.SaveAs(filePath); err != nil {
		return fmt.Errorf("Cannot write file: %s", err)
	}

	fmt.Fprintf(os.Stderr, "Exported %d rows to %s\n", len(rows), filePath)
	return nil
}

func init() {
	rootCmd.AddCommand(exportCmd)
	exportCmd.Flags().StringVar(&exportView, "view", "", "Saved view name")
	exportCmd.Flags().StringVar(&exportMDX, "mdx", "", "MDX query string")
	exportCmd.Flags().StringVarP(&exportOut, "out", "o", "", "Output file path (.csv, .json, .xlsx)")
	exportCmd.Flags().BoolVar(&exportNoHeader, "no-header", false, "Exclude header row from CSV/XLSX output")
}
