package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"time"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var (
	watchInterval string
	watchSeconds  int
)

var watchCmd = &cobra.Command{
	Use:   "watch [flags] -- <command> [command-flags]",
	Short: "Re-execute a command at regular intervals",
	Long: `Re-execute any tm1cli command at regular intervals.

Similar to Unix watch: clears the screen between runs and prints a
timestamp header. Useful for monitoring server state, polling process
status, or watching cube data changes.

Press Ctrl+C to stop.

Note: watch is a terminal-only command and cannot be used with --output json.`,
	Example: `  tm1cli watch -- cubes
  tm1cli watch -n 10 -- process list
  tm1cli watch --interval 30s -- dims --filter region
  tm1cli watch -- cubes --filter sales --count`,
	RunE: runWatch,
}

func resolveExecutable() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("Cannot resolve executable path: %s", err)
	}
	return exe, nil
}

func parseWatchInterval(intervalFlag string, secondsFlag int) (time.Duration, error) {
	if secondsFlag > 0 {
		return time.Duration(secondsFlag) * time.Second, nil
	}
	d, err := time.ParseDuration(intervalFlag)
	if err != nil {
		return 0, fmt.Errorf("Invalid interval '%s'. Use Go duration format (e.g. 5s, 1m, 500ms).", intervalFlag)
	}
	if d < 1*time.Second {
		return 0, fmt.Errorf("Interval must be at least 1s.")
	}
	return d, nil
}

func isTerminal() bool {
	return term.IsTerminal(int(os.Stdout.Fd()))
}

func runWatch(cmd *cobra.Command, args []string) error {
	// Reject JSON output mode (checks flag → env → config → default)
	cfg, _ := loadConfig()
	if isJSONOutput(cfg) {
		output.PrintError("watch is a terminal-only command and cannot be used with --output json.", false)
		return errSilent
	}

	// Require -- separator
	dashIdx := cmd.ArgsLenAtDash()
	if dashIdx < 0 {
		output.PrintError("Missing '--' separator.\nUsage: tm1cli watch [flags] -- <command> [args]", false)
		return errSilent
	}
	subArgs := args[dashIdx:]
	if len(subArgs) == 0 {
		output.PrintError("Missing command after '--'.\nUsage: tm1cli watch [flags] -- <command> [args]", false)
		return errSilent
	}

	// Parse interval
	interval, err := parseWatchInterval(watchInterval, watchSeconds)
	if err != nil {
		output.PrintError(err.Error(), false)
		return errSilent
	}

	// Resolve executable path
	exe, err := resolveExecutable()
	if err != nil {
		output.PrintError(err.Error(), false)
		return errSilent
	}

	// Signal handling for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	label := "tm1cli " + strings.Join(subArgs, " ")
	useClear := isTerminal()

	for {
		if useClear {
			fmt.Print("\033[H\033[2J")
		}
		fmt.Printf("Every %s: %s    %s\n\n", interval, label, time.Now().Format("2006-01-02 15:04:05"))

		child := exec.CommandContext(ctx, exe, subArgs...)
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		_ = child.Run() // non-zero exit keeps loop going (Unix watch behavior)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
}

func init() {
	rootCmd.AddCommand(watchCmd)
	watchCmd.Flags().StringVar(&watchInterval, "interval", "5s", "Polling interval (Go duration: 5s, 1m, 500ms)")
	watchCmd.Flags().IntVarP(&watchSeconds, "seconds", "n", 0, "Polling interval in seconds (shorthand for --interval)")
}
