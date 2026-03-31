package cmd

import (
	"encoding/json"
	"tm1cli/internal/model"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
)

var whoamiCmd = &cobra.Command{
	Use:   "whoami",
	Short: "Show the current TM1 user",
	Long: `Show the currently authenticated TM1 user.

REST API: GET /ActiveUser

Useful for verifying which user account is being used,
especially when managing multiple server connections.`,
	Example: `  tm1cli whoami
  tm1cli whoami --server production
  tm1cli whoami --output json`,
	RunE: runWhoami,
}

func runWhoami(cmd *cobra.Command, args []string) error {
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

	data, err := cl.Get("ActiveUser")
	if err != nil {
		output.PrintError(err.Error(), jsonMode)
		return errSilent
	}

	var user model.ActiveUser
	if err := json.Unmarshal(data, &user); err != nil {
		output.PrintError("Cannot parse server response.", jsonMode)
		return errSilent
	}

	displayWhoami(user, jsonMode)
	return nil
}

func displayWhoami(user model.ActiveUser, jsonMode bool) {
	if jsonMode {
		output.PrintJSON(user)
		return
	}
	headers := []string{"PROPERTY", "VALUE"}
	rows := [][]string{{"User", user.Name}}
	output.PrintTable(headers, rows)
}

func init() {
	rootCmd.AddCommand(whoamiCmd)
}
