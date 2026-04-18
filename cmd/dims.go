package cmd

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

var (
	dimsFilter     string
	dimsLimit      int
	dimsAll        bool
	dimsShowSystem bool
	dimsCount      bool
)

var dimsCmd = &cobra.Command{
	Use:   "dims",
	Short: "List dimensions (Server Explorer → Dimensions in Architect)",
	Long: `List all dimensions on the TM1 server.

Equivalent to: Server Explorer → Dimensions in TM1 Architect
REST API:      GET /Dimensions

System dimensions (starting with }) are hidden by default.`,
	Example: `  tm1cli dims
  tm1cli dims --filter "period"
  tm1cli dims --all
  tm1cli dims --show-system
  tm1cli dims --output json`,
	RunE: runDims,
}

func runDims(cmd *cobra.Command, args []string) error {
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

	showSystem := getShowSystem(cfg, dimsShowSystem)
	limit := getLimit(cfg, dimsLimit, dimsAll)

	endpoint := "Dimensions?$select=Name"

	// Try server-side filter
	filterFallback := false
	if dimsFilter != "" {
		safeFilter := strings.ReplaceAll(dimsFilter, "'", "''")
		filterEndpoint := endpoint + fmt.Sprintf("&$filter=contains(tolower(Name),tolower('%s'))", safeFilter)
		data, err := cl.Get(filterEndpoint)
		if err == nil {
			var resp model.DimensionResponse
			if jsonErr := json.Unmarshal(data, &resp); jsonErr == nil {
				dims := filterSystemDims(resp.Value, showSystem)
				displayDims(dims, len(dims), limit, jsonMode)
				return nil
			}
		}
		filterFallback = true
		output.PrintWarning("Server-side filter not supported, filtering locally...")
	}

	fetchEndpoint := endpoint
	if limit > 0 && dimsFilter == "" {
		fetchEndpoint += fmt.Sprintf("&$top=%d", limit+500)
	}

	data, err := cl.Get(fetchEndpoint)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var resp model.DimensionResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	dims := filterSystemDims(resp.Value, showSystem)

	if dimsFilter != "" && filterFallback {
		dims = filterDimsByName(dims, dimsFilter)
	}

	displayDims(dims, len(dims), limit, jsonMode)
	return nil
}

func filterSystemDims(dims []model.Dimension, showSystem bool) []model.Dimension {
	if showSystem {
		return dims
	}
	var filtered []model.Dimension
	for _, d := range dims {
		if !strings.HasPrefix(d.Name, "}") {
			filtered = append(filtered, d)
		}
	}
	return filtered
}

func filterDimsByName(dims []model.Dimension, filter string) []model.Dimension {
	lower := strings.ToLower(filter)
	var filtered []model.Dimension
	for _, d := range dims {
		if strings.Contains(strings.ToLower(d.Name), lower) {
			filtered = append(filtered, d)
		}
	}
	return filtered
}

func displayDims(dims []model.Dimension, total int, limit int, jsonMode bool) {
	if dimsCount {
		if jsonMode {
			output.PrintJSON(map[string]int{"count": total})
		} else {
			fmt.Printf("%d dimensions\n", total)
		}
		return
	}

	shown := dims
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}

	if jsonMode {
		output.PrintJSON(shown)
		return
	}

	headers := []string{"NAME"}
	rows := make([][]string, len(shown))
	for i, d := range shown {
		rows[i] = []string{d.Name}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(shown), total)
}

// --- dims members ---

var (
	membersHierarchy string
	membersFilter    string
	membersLimit     int
	membersAll       bool
	membersCount     bool
	membersFlat      bool
)

var dimsMembersCmd = &cobra.Command{
	Use:   "members <dimension>",
	Short: "List elements (members) of a dimension",
	Long: `List elements (members) of a dimension.

Equivalent to: Dimension Editor in TM1 Architect
               or expanding a dimension in PAW
REST API:      GET /Dimensions('name')/Hierarchies('name')/Elements

By default uses the dimension's same-named hierarchy.
Use --hierarchy for alternate hierarchies.

Output is indented by default to show hierarchy: children of consolidated
elements are indented two spaces per level. Use --flat for a single-level
list. Indentation is disabled automatically with --filter, --count, and
--output json.

Tree mode fetches the full hierarchy so indentation stays intact; on very
large dimensions (>5000 elements) a stderr warning is emitted recommending
--flat for faster queries.`,
	Example: `  tm1cli dims members Period
  tm1cli dims members Period --flat
  tm1cli dims members Region --hierarchy "Alternate Region"
  tm1cli dims members Period --filter "Q"
  tm1cli dims members Account --output json`,
	Args: cobra.ExactArgs(1),
	RunE: runDimsMembers,
}

func runDimsMembers(cmd *cobra.Command, args []string) error {
	dimName := args[0]

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

	limit := getLimit(cfg, membersLimit, membersAll)

	hierarchy := membersHierarchy
	if hierarchy == "" {
		hierarchy = dimName
	}

	treeMode := !jsonMode && !membersCount && !membersFlat && membersFilter == ""

	endpoint := fmt.Sprintf("Dimensions('%s')/Hierarchies('%s')/Elements?$select=Name,Type", url.PathEscape(dimName), url.PathEscape(hierarchy))
	if treeMode {
		endpoint += "&$expand=Components($select=Name)"
	}

	// Try server-side filter
	filterFallback := false
	if membersFilter != "" {
		safeFilter := strings.ReplaceAll(membersFilter, "'", "''")
		filterEndpoint := endpoint + fmt.Sprintf("&$filter=contains(tolower(Name),tolower('%s'))", safeFilter)
		data, err := cl.Get(filterEndpoint)
		if err == nil {
			var resp model.ElementResponse
			if jsonErr := json.Unmarshal(data, &resp); jsonErr == nil {
				displayMembers(resp.Value, len(resp.Value), limit, jsonMode, treeMode)
				return nil
			}
		}
		filterFallback = true
		output.PrintWarning("Server-side filter not supported, filtering locally...")
	}

	fetchEndpoint := endpoint
	if limit > 0 && membersFilter == "" && !treeMode {
		fetchEndpoint += fmt.Sprintf("&$top=%d", limit)
	}

	data, err := cl.Get(fetchEndpoint)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var resp model.ElementResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	elements := resp.Value

	if membersFilter != "" && filterFallback {
		elements = filterElementsByName(elements, membersFilter)
	}

	if treeMode && len(elements) > treeWarnThreshold {
		output.PrintWarning(fmt.Sprintf("Fetched %d elements; use --flat for faster queries on large dimensions.", len(elements)))
	}

	displayMembers(elements, len(elements), limit, jsonMode, treeMode)
	return nil
}

const treeWarnThreshold = 5000

func filterElementsByName(elements []model.Element, filter string) []model.Element {
	lower := strings.ToLower(filter)
	var filtered []model.Element
	for _, e := range elements {
		if strings.Contains(strings.ToLower(e.Name), lower) {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

func displayMembers(elements []model.Element, total int, limit int, jsonMode bool, treeMode bool) {
	if membersCount {
		if jsonMode {
			output.PrintJSON(map[string]int{"count": total})
		} else {
			fmt.Printf("%d members\n", total)
		}
		return
	}

	if jsonMode {
		shown := elements
		if limit > 0 && len(shown) > limit {
			shown = shown[:limit]
		}
		output.PrintJSON(shown)
		return
	}

	headers := []string{"NAME", "TYPE"}

	if treeMode {
		flat := flattenTree(buildTree(elements))
		flatTotal := len(flat)
		shown := flat
		if limit > 0 && len(shown) > limit {
			shown = shown[:limit]
		}
		rows := make([][]string, len(shown))
		for i, r := range shown {
			rows[i] = []string{strings.Repeat("  ", r.depth) + r.name, r.elType}
		}
		output.PrintTable(headers, rows)
		output.PrintSummary(len(shown), flatTotal)
		return
	}

	shown := elements
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}
	rows := make([][]string, len(shown))
	for i, e := range shown {
		rows[i] = []string{e.Name, e.Type}
	}
	output.PrintTable(headers, rows)
	output.PrintSummary(len(shown), total)
}

// --- hierarchy tree (indentation) ---

type treeNode struct {
	elem     *model.Element
	children []*treeNode
}

type indentedRow struct {
	depth  int
	name   string
	elType string
}

// buildTree turns a flat element list (with populated Components) into a
// forest. Natural roots are elements that never appear as a child; a second
// pass adds synthetic roots for residual or cycle-only subgraphs so no
// element disappears from the output.
func buildTree(elements []model.Element) []*treeNode {
	byName := make(map[string]*model.Element, len(elements))
	childOf := make(map[string]bool)
	for i := range elements {
		byName[elements[i].Name] = &elements[i]
		for _, c := range elements[i].Components {
			childOf[c.Name] = true
		}
	}

	visitedAll := make(map[string]bool)
	var roots []*treeNode

	for i := range elements {
		name := elements[i].Name
		if !childOf[name] && !visitedAll[name] {
			roots = append(roots, buildNode(&elements[i], byName, visitedAll, map[string]bool{}))
		}
	}
	for i := range elements {
		if !visitedAll[elements[i].Name] {
			roots = append(roots, buildNode(&elements[i], byName, visitedAll, map[string]bool{}))
		}
	}
	return roots
}

func buildNode(e *model.Element, byName map[string]*model.Element, visitedAll, visitedPath map[string]bool) *treeNode {
	if visitedPath[e.Name] {
		return &treeNode{elem: e}
	}
	visitedPath[e.Name] = true
	visitedAll[e.Name] = true
	defer delete(visitedPath, e.Name)

	node := &treeNode{elem: e}
	for _, c := range e.Components {
		child, ok := byName[c.Name]
		if !ok {
			continue
		}
		node.children = append(node.children, buildNode(child, byName, visitedAll, visitedPath))
	}
	return node
}

func flattenTree(roots []*treeNode) []indentedRow {
	var out []indentedRow
	var dfs func(*treeNode, int)
	dfs = func(n *treeNode, d int) {
		out = append(out, indentedRow{depth: d, name: n.elem.Name, elType: n.elem.Type})
		for _, c := range n.children {
			dfs(c, d+1)
		}
	}
	for _, r := range roots {
		dfs(r, 0)
	}
	return out
}

func init() {
	rootCmd.AddCommand(dimsCmd)
	dimsCmd.AddCommand(dimsMembersCmd)

	dimsCmd.Flags().StringVar(&dimsFilter, "filter", "", "Filter by name (case-insensitive, partial match)")
	dimsCmd.Flags().IntVar(&dimsLimit, "limit", 0, "Max results to show (default from settings)")
	dimsCmd.Flags().BoolVar(&dimsAll, "all", false, "Show all results, no limit")
	dimsCmd.Flags().BoolVar(&dimsShowSystem, "show-system", false, "Include control dimensions (names starting with })")
	dimsCmd.Flags().BoolVar(&dimsCount, "count", false, "Show count only")

	dimsMembersCmd.Flags().StringVar(&membersHierarchy, "hierarchy", "", "Hierarchy name (default: same as dimension name)")
	dimsMembersCmd.Flags().StringVar(&membersFilter, "filter", "", "Filter by name (case-insensitive, partial match)")
	dimsMembersCmd.Flags().IntVar(&membersLimit, "limit", 0, "Max results to show (default from settings)")
	dimsMembersCmd.Flags().BoolVar(&membersAll, "all", false, "Show all members, no limit")
	dimsMembersCmd.Flags().BoolVar(&membersCount, "count", false, "Show count only")
	dimsMembersCmd.Flags().BoolVar(&membersFlat, "flat", false, "Disable hierarchy indentation (default indents consolidated children)")
}
