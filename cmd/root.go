package cmd

import (
	"errors"
	"fmt"
	"os"
	"tm1cli/internal/client"
	"tm1cli/internal/config"

	"github.com/spf13/cobra"
)

// errSilent signals that an error was already printed to stderr.
// Commands return this after calling output.PrintError so that
// Execute() exits non-zero without cobra re-printing the message.
var errSilent = errors.New("")

var (
	Version     = "dev"
	flagServer  string
	flagOutput  string
	flagVerbose bool
)

var rootCmd = &cobra.Command{
	Use:   "tm1cli",
	Short: "A CLI tool for IBM TM1/Planning Analytics REST API",
	Long: `A CLI tool for IBM TM1/Planning Analytics REST API.

Connect to TM1 servers and perform common operations from the terminal.
Equivalent to using TM1 Architect, PAW, or the REST API browser.

Environment Variables:
  TM1CLI_SERVER     Override active connection
  TM1CLI_OUTPUT     Override output format
  TM1CLI_PASSWORD   Override stored password`,
	Version:       Version,
	SilenceErrors: true,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&flagServer, "server", "", "Use a specific connection instead of default")
	rootCmd.PersistentFlags().StringVar(&flagOutput, "output", "", "Output format: table or json")
	rootCmd.PersistentFlags().BoolVar(&flagVerbose, "verbose", false, "Show request details and debug info")

	rootCmd.SetVersionTemplate("tm1cli v{{.Version}}\n")
}

func loadConfig() (*config.Config, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, fmt.Errorf("No connection configured. Run 'tm1cli config add' first.")
	}
	return cfg, nil
}

func getOutputFormat(cfg *config.Config) string {
	if flagOutput != "" {
		return flagOutput
	}
	if envOutput := os.Getenv("TM1CLI_OUTPUT"); envOutput != "" {
		return envOutput
	}
	if cfg != nil {
		return cfg.GetEffectiveOutput()
	}
	return config.DefaultOutput
}

func getLimit(cfg *config.Config, flagLimit int, flagAll bool) int {
	if flagAll {
		return 0
	}
	if flagLimit > 0 {
		return flagLimit
	}
	if cfg != nil && cfg.Settings.DefaultLimit > 0 {
		return cfg.Settings.DefaultLimit
	}
	return config.DefaultLimit
}

func getShowSystem(cfg *config.Config, flagShowSystem bool) bool {
	if flagShowSystem {
		return true
	}
	if cfg != nil {
		return cfg.Settings.ShowSystem
	}
	return config.DefaultShowSystem
}

func isJSONOutput(cfg *config.Config) bool {
	return getOutputFormat(cfg) == "json"
}

func createClient(cfg *config.Config) (*client.Client, error) {
	serverName := flagServer
	if serverName == "" {
		serverName = cfg.GetEffectiveServer()
	}

	srv, err := cfg.GetServer(serverName)
	if err != nil {
		return nil, err
	}

	password, err := cfg.GetEffectivePassword(serverName)
	if err != nil {
		return nil, err
	}

	return client.NewClient(*srv, password, cfg.Settings.TLSVerify, flagVerbose)
}
