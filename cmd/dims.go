package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"tm1cli/internal/client"
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

// treeElementGate is the server-side element count above which `dims members`
// refuses to render a tree unless --all is passed. Keeps the default
// invocation fast on huge dimensions.
//
// treeRenderCeiling is an absolute cap on materialised tree nodes to guard
// against layered-diamond hierarchies whose expanded render size is far
// larger than the unique element count reported by $count.
const (
	treeElementGate   = 5000
	treeRenderCeiling = 50000
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

Tree mode fetches the full hierarchy so indentation stays intact. For
dimensions over 5000 elements the command falls back to flat output
with a warning, keeping the pre-PR contract of the default invocation;
pass --all to get the full indented tree. If the server doesn't support
/Elements/$count the command falls back to flat output the same way.
Hierarchies with extreme diamond expansion (over 50000 render rows)
are rejected unconditionally — use --flat to inspect them.`,
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

	if treeMode && !membersAll {
		countEndpoint := fmt.Sprintf("Dimensions('%s')/Hierarchies('%s')/Elements/$count", url.PathEscape(dimName), url.PathEscape(hierarchy))
		data, err := cl.Get(countEndpoint)
		switch {
		case err != nil:
			if errors.Is(err, client.ErrNotFound) {
				output.PrintError(err.Error(), jsonMode)
				return errSilent
			}
			output.PrintWarning(preflightFallbackMessage("cannot verify dimension size", limit))
			treeMode = false
		default:
			n, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
			switch {
			case parseErr != nil:
				output.PrintWarning(preflightFallbackMessage("cannot verify dimension size (unexpected server response)", limit))
				treeMode = false
			case n > treeElementGate:
				output.PrintWarning(preflightFallbackMessage(fmt.Sprintf("dimension has %d elements (over %d)", n, treeElementGate), limit))
				treeMode = false
			}
		}
	}

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
				return displayMembers(resp.Value, len(resp.Value), limit, jsonMode, treeMode)
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

	return displayMembers(elements, len(elements), limit, jsonMode, treeMode)
}

// preflightFallbackMessage composes the stderr warning printed when the
// $count preflight cannot be satisfied. The warning must surface the row
// limit explicitly so users don't silently consume truncated results.
func preflightFallbackMessage(reason string, limit int) string {
	if limit > 0 {
		return fmt.Sprintf("%s; falling back to flat output (limited to %d rows; use --all for the full dimension).", reason, limit)
	}
	return fmt.Sprintf("%s; falling back to flat output (use --all for the full dimension).", reason)
}

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

func displayMembers(elements []model.Element, total int, limit int, jsonMode bool, treeMode bool) error {
	if membersCount {
		if jsonMode {
			output.PrintJSON(map[string]int{"count": total})
		} else {
			fmt.Printf("%d members\n", total)
		}
		return nil
	}

	if jsonMode {
		shown := elements
		if limit > 0 && len(shown) > limit {
			shown = shown[:limit]
		}
		output.PrintJSON(shown)
		return nil
	}

	headers := []string{"NAME", "TYPE"}

	if treeMode {
		roots, overflow := buildTreeBounded(elements, treeRenderCeiling)
		if overflow {
			output.PrintError(fmt.Sprintf("hierarchy would expand to more than %d render rows (likely from layered diamonds); use --flat.", treeRenderCeiling), jsonMode)
			return errSilent
		}
		flatTotal, uniqueCount := treeStats(roots)
		shown := flattenTreeCapped(roots, limit)
		rows := make([][]string, len(shown))
		for i, r := range shown {
			rows[i] = []string{strings.Repeat("  ", r.depth) + r.name, r.elType}
		}
		output.PrintTable(headers, rows)
		output.PrintTreeSummary(len(shown), flatTotal, uniqueCount)
		return nil
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
	return nil
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
// forest. Natural roots are elements that never appear as a child; a
// second pass adds synthetic roots for residual or cycle-only subgraphs
// so no element disappears from the output. Cycles are broken by a
// per-DFS-path visited set — diamonds (same child under multiple
// parents) render in every location because that set is scoped to a
// single walk, not shared across parents.
func buildTree(elements []model.Element) []*treeNode {
	roots, _ := buildTreeBounded(elements, 0)
	return roots
}

// buildTreeBounded is the same as buildTree but caps the total number
// of materialised tree nodes at budget. When the budget is exceeded it
// stops walking and returns overflow=true so callers can bail out
// before allocating a dimension-size worth of nodes. Pass budget <= 0
// for no cap.
func buildTreeBounded(elements []model.Element, budget int) (roots []*treeNode, overflow bool) {
	byName := make(map[string]*model.Element, len(elements))
	childOf := make(map[string]bool)
	for i := range elements {
		byName[elements[i].Name] = &elements[i]
		for _, c := range elements[i].Components {
			childOf[c.Name] = true
		}
	}

	ctx := &treeBuildCtx{
		byName:     byName,
		visitedAll: make(map[string]bool),
		budget:     budget,
	}

	for i := range elements {
		name := elements[i].Name
		if ctx.over() {
			break
		}
		if !childOf[name] && !ctx.visitedAll[name] {
			roots = append(roots, buildNode(&elements[i], ctx, map[string]bool{}))
		}
	}
	for i := range elements {
		if ctx.over() {
			break
		}
		if !ctx.visitedAll[elements[i].Name] {
			roots = append(roots, buildNode(&elements[i], ctx, map[string]bool{}))
		}
	}
	return roots, ctx.overflow
}

type treeBuildCtx struct {
	byName     map[string]*model.Element
	visitedAll map[string]bool
	budget     int
	count      int
	overflow   bool
}

func (c *treeBuildCtx) over() bool {
	return c.overflow
}

func buildNode(e *model.Element, ctx *treeBuildCtx, visitedPath map[string]bool) *treeNode {
	ctx.count++
	if ctx.budget > 0 && ctx.count > ctx.budget {
		ctx.overflow = true
		return &treeNode{elem: e}
	}
	if visitedPath[e.Name] {
		return &treeNode{elem: e}
	}
	visitedPath[e.Name] = true
	ctx.visitedAll[e.Name] = true
	defer delete(visitedPath, e.Name)

	node := &treeNode{elem: e}
	for _, c := range e.Components {
		if ctx.overflow {
			break
		}
		child, ok := ctx.byName[c.Name]
		if !ok {
			continue
		}
		node.children = append(node.children, buildNode(child, ctx, visitedPath))
	}
	return node
}

func flattenTree(roots []*treeNode) []indentedRow {
	return flattenTreeCapped(roots, 0)
}

// flattenTreeCapped walks the tree in DFS preorder and stops materializing
// rows once budget rows have been emitted. Pass budget <= 0 for no cap.
// The DFS itself short-circuits — callers still needing a full flatTotal
// should use treeStats for a count-only pass. (Named budget, not cap,
// because cap() is a Go builtin and a parameter named cap would shadow
// it inside the function body.)
func flattenTreeCapped(roots []*treeNode, budget int) []indentedRow {
	var out []indentedRow
	var dfs func(*treeNode, int) bool
	dfs = func(n *treeNode, d int) bool {
		out = append(out, indentedRow{depth: d, name: n.elem.Name, elType: n.elem.Type})
		if budget > 0 && len(out) >= budget {
			return false
		}
		for _, c := range n.children {
			if !dfs(c, d+1) {
				return false
			}
		}
		return true
	}
	for _, r := range roots {
		if !dfs(r, 0) {
			return out
		}
	}
	return out
}

// treeStats counts total DFS rows and unique element names without
// materializing row structs.
func treeStats(roots []*treeNode) (flatTotal int, uniqueCount int) {
	seen := make(map[string]struct{})
	var dfs func(*treeNode)
	dfs = func(n *treeNode) {
		flatTotal++
		seen[n.elem.Name] = struct{}{}
		for _, c := range n.children {
			dfs(c)
		}
	}
	for _, r := range roots {
		dfs(r)
	}
	return flatTotal, len(seen)
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
