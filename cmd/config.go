package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"tm1cli/internal/client"
	"tm1cli/internal/config"
	"tm1cli/internal/output"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage TM1 server connections",
	Long: `Manage TM1 server connections.

Similar to managing server entries in TM1 Architect's "Add New Server" dialog.
Supports multiple servers with easy switching between them.`,
}

// --- config add ---

var (
	addFlagURL       string
	addFlagUser      string
	addFlagPassword  string
	addFlagAuth      string
	addFlagNamespace string
)

var configAddCmd = &cobra.Command{
	Use:   "add [name]",
	Short: "Add a new TM1 server connection",
	Long: `Add a new TM1 server connection.

Runs interactively by default, prompting for each field step by step.
Use flags to skip prompts (useful for scripting).

The auth mode maps to IntegratedSecurityMode in tm1s.cfg:
  basic = Mode 1 (TM1 native security)
  cam   = Mode 4 or 5 (CAM/LDAP)

Password is stored base64-encoded (not encrypted).
For better security, use the TM1CLI_PASSWORD environment variable.`,
	Example: `  # interactive (recommended for first time)
  tm1cli config add

  # with flags
  tm1cli config add myserver --url https://host:8010 --user myuser --auth basic

  # with env var for password (no storage)
  export TM1CLI_PASSWORD=mysecret
  tm1cli config add myserver --url https://host:8010/api/v1 --user myuser --auth basic`,
	Args: cobra.MaximumNArgs(1),
	RunE: runConfigAdd,
}

func runConfigAdd(cmd *cobra.Command, args []string) error {
	reader := bufio.NewReader(os.Stdin)

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg == nil {
		cfg = config.NewConfig()
	}

	// Connection name
	name := ""
	if len(args) > 0 {
		name = args[0]
	}
	if name == "" {
		fmt.Print("Connection name: ")
		name, _ = reader.ReadString('\n')
		name = strings.TrimSpace(name)
	}
	if name == "" {
		return fmt.Errorf("connection name is required")
	}

	// Check for existing
	if _, exists := cfg.Servers[name]; exists {
		fmt.Printf("Connection '%s' already exists. Use 'tm1cli config edit %s' to modify.\n", name, name)
		return nil
	}

	// URL
	url := addFlagURL
	if url == "" {
		fmt.Print("TM1 Server URL (e.g. https://host:8010): ")
		url, _ = reader.ReadString('\n')
		url = strings.TrimSpace(url)
	}
	if url == "" {
		return fmt.Errorf("URL is required")
	}

	// Auth mode
	authMode := addFlagAuth
	if authMode == "" {
		fmt.Print("Auth mode (basic/cam) [basic]: ")
		authMode, _ = reader.ReadString('\n')
		authMode = strings.TrimSpace(authMode)
	}
	if authMode == "" {
		authMode = "basic"
	}
	authMode = strings.ToLower(authMode)
	if authMode != "basic" && authMode != "cam" {
		return fmt.Errorf("auth mode must be 'basic' or 'cam'")
	}

	// Namespace (CAM only)
	namespace := addFlagNamespace
	if authMode == "cam" && namespace == "" {
		fmt.Print("CAM namespace: ")
		namespace, _ = reader.ReadString('\n')
		namespace = strings.TrimSpace(namespace)
	}

	// Username
	user := addFlagUser
	if user == "" {
		fmt.Print("Username: ")
		user, _ = reader.ReadString('\n')
		user = strings.TrimSpace(user)
	}
	if user == "" {
		return fmt.Errorf("username is required")
	}

	// Password
	password := addFlagPassword
	if password == "" {
		if envPass := os.Getenv("TM1CLI_PASSWORD"); envPass != "" {
			password = envPass
		} else {
			fmt.Print("Password: ")
			pwBytes, err := term.ReadPassword(int(os.Stdin.Fd()))
			fmt.Println()
			if err != nil {
				return fmt.Errorf("cannot read password: %w", err)
			}
			password = string(pwBytes)
		}
	}

	srv := config.ServerConfig{
		URL:       url,
		User:      user,
		Password:  config.EncodePassword(password),
		AuthMode:  authMode,
		Namespace: namespace,
	}

	// Test connection
	fmt.Print("Testing connection... ")
	testClient, err := createClientFromServerConfig(srv, password, cfg.Settings.TLSVerify)
	if err != nil {
		fmt.Println("✗")
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		if !promptYesNo(reader, "Save anyway?") {
			return nil
		}
	} else {
		_, testErr := testClient.Get("Cubes?$top=1")
		if testErr != nil {
			fmt.Println("✗")
			fmt.Fprintf(os.Stderr, "Error: %s\n", testErr)
			if !promptYesNo(reader, "Save anyway?") {
				return nil
			}
		} else {
			fmt.Println("✓")
		}
	}

	isFirst := len(cfg.Servers) == 0
	cfg.AddServer(name, srv)

	if err := config.Save(cfg); err != nil {
		return fmt.Errorf("cannot save config: %w", err)
	}

	if isFirst {
		fmt.Printf("Connection '%s' added and set as default.\n", name)
	} else {
		fmt.Printf("Connection '%s' added.\n", name)
	}
	fmt.Println("Note: Password is stored base64-encoded (not encrypted).")
	fmt.Println("      For better security, use TM1CLI_PASSWORD env var instead.")

	return nil
}

// --- config list ---

var configListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all saved connections",
	Long: `List all saved TM1 server connections.

The active connection is marked with *.`,
	RunE: runConfigList,
}

func runConfigList(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg == nil || len(cfg.Servers) == 0 {
		fmt.Println("No connections configured. Run 'tm1cli config add' to add one.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	for name, srv := range cfg.Servers {
		marker := " "
		if name == cfg.Default {
			marker = "*"
		}
		authLabel := srv.AuthMode
		if srv.AuthMode == "cam" && srv.Namespace != "" {
			authLabel = "cam/" + srv.Namespace
		}
		fmt.Fprintf(w, "%s %s\t%s\t(%s)\t%s\n", marker, name, srv.URL, srv.User, authLabel)
	}
	w.Flush()
	return nil
}

// --- config use ---

var configUseCmd = &cobra.Command{
	Use:   "use <name>",
	Short: "Switch active connection",
	Long: `Switch the active TM1 server connection.

Like connecting to a different server in TM1 Architect.
All subsequent commands will use this connection unless overridden with --server.`,
	Example: `  tm1cli config use production
  tm1cli cubes                    # now hits production server`,
	Args: cobra.ExactArgs(1),
	RunE: runConfigUse,
}

func runConfigUse(cmd *cobra.Command, args []string) error {
	name := args[0]

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg == nil {
		return fmt.Errorf("No connection configured. Run 'tm1cli config add' first.")
	}

	if _, ok := cfg.Servers[name]; !ok {
		return fmt.Errorf("Connection '%s' not found. Run 'tm1cli config list' to see available.", name)
	}

	cfg.Default = name
	if err := config.Save(cfg); err != nil {
		return err
	}

	fmt.Printf("Switched to '%s'.\n", name)
	return nil
}

// --- config edit (TODO: Phase 2) ---

var configEditCmd = &cobra.Command{
	Use:   "edit <name>",
	Short: "Edit an existing connection",
	Long: `Edit an existing TM1 server connection.

Shows current values in brackets. Press Enter to keep existing value.
Re-tests connection after editing.`,
	Example: `  tm1cli config edit myserver`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// TODO: Phase 2 — implement interactive edit with current values shown
		return fmt.Errorf("config edit is not yet implemented (coming in v0.2.0)")
	},
}

// --- config remove ---

var configRemoveCmd = &cobra.Command{
	Use:   "remove <name>",
	Short: "Remove a connection",
	Long: `Remove a saved TM1 server connection.

Asks for confirmation before removing.
If the active connection is removed, switches to another available connection.`,
	Example: `  tm1cli config remove old_server`,
	Args:    cobra.ExactArgs(1),
	RunE:    runConfigRemove,
}

func runConfigRemove(cmd *cobra.Command, args []string) error {
	name := args[0]
	reader := bufio.NewReader(os.Stdin)

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg == nil {
		return fmt.Errorf("No connection configured.")
	}

	if _, ok := cfg.Servers[name]; !ok {
		return fmt.Errorf("Connection '%s' not found.", name)
	}

	if !promptYesNo(reader, fmt.Sprintf("Remove connection '%s'?", name)) {
		return nil
	}

	newDefault := cfg.RemoveServer(name)

	if err := config.Save(cfg); err != nil {
		return err
	}

	if newDefault != "" {
		fmt.Printf("Removed '%s'. Switched default to '%s'.\n", name, newDefault)
	} else {
		fmt.Printf("Removed '%s'. No connections remaining.\n", name)
	}
	return nil
}

// --- config settings ---

var (
	settingsLimit  int
	settingsOutput string
	settingsSystem bool
	settingsTLS    bool
	settingsReset  bool
)

var configSettingsCmd = &cobra.Command{
	Use:   "settings",
	Short: "View or change default settings",
	Long: `View or change default settings.

These defaults apply to all commands unless overridden by flags or environment variables.

Built-in defaults:
  limit:       50
  output:      table
  show-system: false
  tls-verify:  false`,
	Example: `  tm1cli config settings                    # view current
  tm1cli config settings --limit 100        # change default limit
  tm1cli config settings --output json      # change default output
  tm1cli config settings --reset            # reset all to defaults`,
	RunE: runConfigSettings,
}

func runConfigSettings(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg == nil {
		cfg = config.NewConfig()
	}

	hasChanges := settingsReset ||
		cmd.Flags().Changed("limit") ||
		cmd.Flags().Changed("output") ||
		cmd.Flags().Changed("show-system") ||
		cmd.Flags().Changed("tls-verify")

	if settingsReset {
		cfg.Settings = config.DefaultSettings()
		if err := config.Save(cfg); err != nil {
			return err
		}
		fmt.Println("Settings reset to defaults.")
		return nil
	}

	if hasChanges {
		if cmd.Flags().Changed("limit") {
			cfg.Settings.DefaultLimit = settingsLimit
		}
		if cmd.Flags().Changed("output") {
			cfg.Settings.OutputFormat = settingsOutput
		}
		if cmd.Flags().Changed("show-system") {
			cfg.Settings.ShowSystem = settingsSystem
		}
		if cmd.Flags().Changed("tls-verify") {
			cfg.Settings.TLSVerify = settingsTLS
		}
		if err := config.Save(cfg); err != nil {
			return err
		}
		fmt.Println("Settings updated.")
		return nil
	}

	// Show current settings
	jsonMode := isJSONOutput(cfg)
	if jsonMode {
		output.PrintJSON(cfg.Settings)
	} else {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintf(w, "SETTING\tVALUE\n")
		fmt.Fprintf(w, "limit\t%d\n", cfg.Settings.DefaultLimit)
		fmt.Fprintf(w, "output\t%s\n", cfg.Settings.OutputFormat)
		fmt.Fprintf(w, "show-system\t%t\n", cfg.Settings.ShowSystem)
		fmt.Fprintf(w, "tls-verify\t%t\n", cfg.Settings.TLSVerify)
		w.Flush()
	}
	return nil
}

func init() {
	rootCmd.AddCommand(configCmd)
	configCmd.AddCommand(configAddCmd)
	configCmd.AddCommand(configListCmd)
	configCmd.AddCommand(configUseCmd)
	configCmd.AddCommand(configEditCmd)
	configCmd.AddCommand(configRemoveCmd)
	configCmd.AddCommand(configSettingsCmd)

	// config add flags
	configAddCmd.Flags().StringVar(&addFlagURL, "url", "", "TM1 server URL (https://host:port)")
	configAddCmd.Flags().StringVar(&addFlagUser, "user", "", "Username")
	configAddCmd.Flags().StringVar(&addFlagPassword, "password", "", "Password (prefer TM1CLI_PASSWORD env var for security)")
	configAddCmd.Flags().StringVar(&addFlagAuth, "auth", "", "Auth mode: basic or cam (default: basic)")
	configAddCmd.Flags().StringVar(&addFlagNamespace, "namespace", "", "CAM namespace (required for cam auth)")

	// config settings flags
	configSettingsCmd.Flags().IntVar(&settingsLimit, "limit", 0, "Default result limit")
	configSettingsCmd.Flags().StringVar(&settingsOutput, "output", "", "Default output format: table or json")
	configSettingsCmd.Flags().BoolVar(&settingsSystem, "show-system", false, "Show system objects by default")
	configSettingsCmd.Flags().BoolVar(&settingsTLS, "tls-verify", false, "Verify TLS certificates")
	configSettingsCmd.Flags().BoolVar(&settingsReset, "reset", false, "Reset all settings to defaults")
}

// helpers

func promptYesNo(reader *bufio.Reader, prompt string) bool {
	fmt.Printf("%s (y/N) ", prompt)
	answer, _ := reader.ReadString('\n')
	answer = strings.TrimSpace(strings.ToLower(answer))
	return answer == "y" || answer == "yes"
}

func createClientFromServerConfig(srv config.ServerConfig, password string, tlsVerify bool) (*client.Client, error) {
	return client.NewClient(srv, password, tlsVerify, false)
}
