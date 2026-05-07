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

func PrintSummary(shown int, total int, hints ...string) {
	if shown < total {
		hint := "--filter to search or --all"
		if len(hints) > 0 && hints[0] != "" {
			hint = hints[0]
		}
		fmt.Fprintf(os.Stderr, "Showing %d of %d. Use %s to show everything.\n", shown, total, hint)
	}
}

// UniqueElementsUnknown signals to PrintTreeSummary that the caller did
// not compute a unique-element count; the "(N unique elements)" clause
// will be suppressed.
const UniqueElementsUnknown = -1

// PrintTreeSummary is the tree-mode equivalent of PrintSummary. The
// displayed count is paths ("rows") rather than unique elements because
// diamond hierarchies render shared children under every parent; the
// unique-element count is appended when it differs so users aren't
// misled into thinking "Showing 3 of 7" means 7 distinct members. Pass
// UniqueElementsUnknown (or a value equal to totalRows) to suppress the
// unique-count suffix.
func PrintTreeSummary(shownRows int, totalRows int, uniqueElements int) {
	if shownRows >= totalRows {
		return
	}
	if uniqueElements != UniqueElementsUnknown && uniqueElements != totalRows {
		fmt.Fprintf(os.Stderr, "Showing %d of %d rows (%d unique elements). Use --filter to search or --all to show everything.\n", shownRows, totalRows, uniqueElements)
		return
	}
	fmt.Fprintf(os.Stderr, "Showing %d of %d rows. Use --filter to search or --all to show everything.\n", shownRows, totalRows)
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
