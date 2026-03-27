package output

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
)

func PrintTable(headers []string, rows [][]string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, strings.Join(headers, "\t"))
	for _, row := range rows {
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}
	w.Flush()
}

func PrintJSON(data interface{}) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(data)
}

func PrintSummary(shown int, total int) {
	if shown < total {
		fmt.Fprintf(os.Stderr, "Showing %d of %d. Use --filter to search or --all to show everything.\n", shown, total)
	}
}

func PrintError(msg string, jsonMode bool) {
	if jsonMode {
		errObj := map[string]interface{}{"error": msg}
		data, _ := json.MarshalIndent(errObj, "", "  ")
		fmt.Fprintln(os.Stderr, string(data))
	} else {
		fmt.Fprintf(os.Stderr, "Error: %s\n", msg)
	}
}

func PrintErrorWithStatus(msg string, status int, jsonMode bool) {
	if jsonMode {
		errObj := map[string]interface{}{"error": msg, "status": status}
		data, _ := json.MarshalIndent(errObj, "", "  ")
		fmt.Fprintln(os.Stderr, string(data))
	} else {
		fmt.Fprintf(os.Stderr, "Error (HTTP %d): %s\n", status, msg)
	}
}

func PrintWarning(msg string) {
	fmt.Fprintf(os.Stderr, "[warn] %s\n", msg)
}
