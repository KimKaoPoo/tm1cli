package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"tm1cli/internal/client"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

var (
	saveDataYes     bool
	saveDataDryRun  bool
	saveDataWait    bool
	saveDataTimeout time.Duration
)

const defaultSaveDataTimeout = 5 * time.Minute

type SaveDataResult struct {
	Status    string    `json:"status"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	ElapsedMS int64     `json:"elapsed_ms"`
	Elapsed   string    `json:"elapsed"`
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Server-wide TM1 operations",
	Long:  `Server-wide operations against the TM1 server (e.g. SaveDataAll).`,
}

var serverSaveDataCmd = &cobra.Command{
	Use:   "save-data",
	Short: "Trigger a server-wide SaveDataAll",
	Long: `Trigger a server-wide SaveDataAll on the TM1 server.

REST API: POST /SaveDataAll

WARNING: SaveDataAll briefly pauses user activity on the server while the
in-memory state is flushed to disk. On large models this can take several
minutes. The command prompts for confirmation unless --yes is given.

The TM1 SaveDataAll endpoint is synchronous: this command always waits for
completion. Use --timeout to cap the wait (default 5m). The --wait flag is
accepted for forward compatibility and is currently a no-op.`,
	Example: `  tm1cli server save-data
  tm1cli server save-data --yes
  tm1cli server save-data --timeout 10m
  tm1cli server save-data --dry-run
  tm1cli server save-data --output json --yes`,
	RunE: runServerSaveData,
}

func runServerSaveData(cmd *cobra.Command, args []string) error {
	cfg, err := loadConfig()
	if err != nil {
		output.PrintError(err.Error(), isJSONOutput(nil))
		return errSilent
	}
	jsonMode := isJSONOutput(cfg)

	if saveDataTimeout <= 0 {
		output.PrintError("--timeout must be greater than 0.", jsonMode)
		return errSilent
	}

	if saveDataDryRun {
		if jsonMode {
			output.PrintJSON(map[string]string{"status": "dry-run"})
		} else {
			fmt.Println("[dry-run] Would POST /SaveDataAll.")
		}
		return nil
	}

	if !saveDataYes {
		// promptYesNo returns false on EOF, so a closed stdin (script with no --yes) declines safely.
		fmt.Fprintln(os.Stderr, "SaveDataAll will briefly pause user activity on the TM1 server while data is flushed to disk.")
		if !promptYesNo(bufio.NewReader(os.Stdin), "Continue?") {
			return nil
		}
	}

	cl, err := createClient(cfg)
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}
	cl.SetTimeout(saveDataTimeout)

	// Use monotonic time for elapsed (NTP/clock-jump safe); UTC times are for display.
	startMono := time.Now()
	start := startMono.UTC()
	_, err = cl.Post("SaveDataAll", nil)
	elapsed := time.Since(startMono)
	end := startMono.Add(elapsed).UTC()

	if err != nil {
		switch {
		case errors.Is(err, client.ErrTimeout):
			output.PrintError(
				fmt.Sprintf("SaveDataAll did not complete within %s. The save may still be in progress on the server.", saveDataTimeout),
				jsonMode,
			)
		case errors.Is(err, client.ErrNotFound):
			output.PrintError("SaveDataAll endpoint not found on this server.", jsonMode)
		case strings.HasPrefix(err.Error(), "HTTP 403"):
			output.PrintError("Permission denied. SaveDataAll requires admin privileges.", jsonMode)
		default:
			// 401 lands here so the existing "Authentication failed" message bubbles up unchanged.
			output.PrintError(err.Error(), jsonMode)
		}
		return errSilent
	}

	displaySaveDataResult(SaveDataResult{
		Status:    "ok",
		Start:     start,
		End:       end,
		ElapsedMS: elapsed.Milliseconds(),
		Elapsed:   elapsed.Round(time.Millisecond).String(),
	}, jsonMode)
	return nil
}

func displaySaveDataResult(r SaveDataResult, jsonMode bool) {
	if jsonMode {
		output.PrintJSON(r)
		return
	}
	output.PrintTable(
		[]string{"PROPERTY", "VALUE"},
		[][]string{
			{"Status", r.Status},
			{"Start", r.Start.Format(time.RFC3339)},
			{"End", r.End.Format(time.RFC3339)},
			{"Elapsed", r.Elapsed},
		},
	)
}

func init() {
	rootCmd.AddCommand(serverCmd)
	serverCmd.AddCommand(serverSaveDataCmd)

	serverSaveDataCmd.Flags().BoolVar(&saveDataYes, "yes", false, "Skip confirmation prompt")
	serverSaveDataCmd.Flags().BoolVar(&saveDataDryRun, "dry-run", false, "Preview without contacting the server")
	serverSaveDataCmd.Flags().BoolVar(&saveDataWait, "wait", false, "Block on completion (currently always blocks; reserved for future async support)")
	serverSaveDataCmd.Flags().DurationVar(&saveDataTimeout, "timeout", defaultSaveDataTimeout, "Maximum time to wait for SaveDataAll to complete")
}
