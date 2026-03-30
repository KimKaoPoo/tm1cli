package cmd

import (
	"encoding/json"
	"fmt"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

var serverInfoCmd = &cobra.Command{
	Use:   "server-info",
	Short: "Show TM1 server details",
	Long: `Show TM1 server version, name, host, and port.

REST API: GET /Configuration

Useful for verifying server connectivity and checking
the TM1 server version and configuration.`,
	Example: `  tm1cli server-info
  tm1cli server-info --server production
  tm1cli server-info --output json`,
	RunE: runServerInfo,
}

func runServerInfo(cmd *cobra.Command, args []string) error {
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

	data, err := cl.Get("Configuration")
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var serverCfg model.ServerConfiguration
	if err := json.Unmarshal(data, &serverCfg); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	displayServerInfo(serverCfg, jsonMode)
	return nil
}

func displayServerInfo(serverCfg model.ServerConfiguration, jsonMode bool) {
	if jsonMode {
		output.PrintJSON(serverCfg)
		return
	}
	headers := []string{"PROPERTY", "VALUE"}
	rows := [][]string{
		{"Server Name", serverCfg.ServerName},
		{"Version", serverCfg.ProductVersion},
		{"Admin Host", serverCfg.AdminHost},
		{"HTTP Port", fmt.Sprintf("%d", serverCfg.HTTPPortNumber)},
	}
	output.PrintTable(headers, rows)
}

func init() {
	rootCmd.AddCommand(serverInfoCmd)
}
