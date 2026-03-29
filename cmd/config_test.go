package cmd

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"tm1cli/internal/config"
)

// setupTestHome creates a temp directory, sets HOME, and optionally writes a config file.
func setupTestHome(t *testing.T, cfg *config.Config) {
	t.Helper()
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)

	if cfg != nil {
		cfgDir := filepath.Join(tmpDir, ".tm1cli")
		if err := os.MkdirAll(cfgDir, 0700); err != nil {
			t.Fatalf("cannot create config dir: %v", err)
		}
		data, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			t.Fatalf("cannot marshal config: %v", err)
		}
		if err := os.WriteFile(filepath.Join(cfgDir, "config.json"), data, 0600); err != nil {
			t.Fatalf("cannot write config: %v", err)
		}
	}
}

// withStdin replaces os.Stdin with a reader containing input for the duration of fn.
func withStdin(t *testing.T, input string, fn func()) {
	t.Helper()
	old := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("cannot create pipe: %v", err)
	}
	go func() {
		w.Write([]byte(input))
		w.Close()
	}()
	os.Stdin = r
	defer func() { os.Stdin = old }()
	fn()
}

// testConfig returns a Config with two test servers ("dev" active, "prod").
func testConfig() *config.Config {
	return &config.Config{
		Default:  "dev",
		Settings: config.DefaultSettings(),
		Servers: map[string]config.ServerConfig{
			"dev": {
				URL:      "https://dev-server:8010/api/v1",
				User:     "admin",
				Password: config.EncodePassword("secret"),
				AuthMode: "basic",
			},
			"prod": {
				URL:       "https://prod-server:8010/api/v1",
				User:      "admin",
				Password:  config.EncodePassword("secret"),
				AuthMode:  "cam",
				Namespace: "LDAP",
			},
		},
	}
}

// saveAddFlags saves current add-flag values and returns a restore function.
func saveAddFlags() func() {
	u, usr, pw, auth, ns := addFlagURL, addFlagUser, addFlagPassword, addFlagAuth, addFlagNamespace
	return func() {
		addFlagURL, addFlagUser, addFlagPassword, addFlagAuth, addFlagNamespace = u, usr, pw, auth, ns
	}
}

// --- config list ---

func TestConfigList(t *testing.T) {
	t.Run("shows connections with active marker", func(t *testing.T) {
		setupTestHome(t, testConfig())

		output := captureStdout(t, func() {
			if err := runConfigList(configListCmd, nil); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(output, "* dev") {
			t.Errorf("should mark dev as active, got: %q", output)
		}
		if !strings.Contains(output, "prod") {
			t.Errorf("should list prod, got: %q", output)
		}
		if !strings.Contains(output, "https://dev-server:8010/api/v1") {
			t.Errorf("should show dev URL, got: %q", output)
		}
	})

	t.Run("no connections shows help message", func(t *testing.T) {
		setupTestHome(t, nil)

		output := captureStdout(t, func() {
			if err := runConfigList(configListCmd, nil); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(output, "No connections configured") {
			t.Errorf("should say no connections, got: %q", output)
		}
	})

	t.Run("empty config shows help message", func(t *testing.T) {
		setupTestHome(t, config.NewConfig())

		output := captureStdout(t, func() {
			if err := runConfigList(configListCmd, nil); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(output, "No connections configured") {
			t.Errorf("should say no connections, got: %q", output)
		}
	})

	t.Run("shows CAM namespace in auth label", func(t *testing.T) {
		setupTestHome(t, testConfig())

		output := captureStdout(t, func() {
			if err := runConfigList(configListCmd, nil); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(output, "cam/LDAP") {
			t.Errorf("should show cam/LDAP for prod, got: %q", output)
		}
	})
}

// --- config use ---

func TestConfigUse(t *testing.T) {
	t.Run("switches to existing connection", func(t *testing.T) {
		setupTestHome(t, testConfig())

		output := captureStdout(t, func() {
			if err := runConfigUse(configUseCmd, []string{"prod"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(output, "Switched to 'prod'") {
			t.Errorf("should confirm switch, got: %q", output)
		}

		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		if cfg.Default != "prod" {
			t.Errorf("default = %q, want %q", cfg.Default, "prod")
		}
	})

	t.Run("error for non-existent connection", func(t *testing.T) {
		setupTestHome(t, testConfig())

		err := runConfigUse(configUseCmd, []string{"nonexistent"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("error = %q, want it to contain 'not found'", err.Error())
		}
	})

	t.Run("error when no config exists", func(t *testing.T) {
		setupTestHome(t, nil)

		err := runConfigUse(configUseCmd, []string{"dev"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "No connection configured") {
			t.Errorf("error = %q, want 'No connection configured'", err.Error())
		}
	})
}

// --- config remove ---

func TestConfigRemove(t *testing.T) {
	t.Run("removes non-active connection with confirmation", func(t *testing.T) {
		setupTestHome(t, testConfig())

		var output string
		withStdin(t, "y\n", func() {
			output = captureStdout(t, func() {
				if err := runConfigRemove(configRemoveCmd, []string{"prod"}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		})

		if !strings.Contains(output, "Removed 'prod'") {
			t.Errorf("should confirm removal, got: %q", output)
		}

		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		if _, exists := cfg.Servers["prod"]; exists {
			t.Error("prod should have been removed")
		}
		if cfg.Default != "dev" {
			t.Errorf("default should still be dev, got %q", cfg.Default)
		}
	})

	t.Run("removes active connection and switches default", func(t *testing.T) {
		setupTestHome(t, testConfig())

		var output string
		withStdin(t, "y\n", func() {
			output = captureStdout(t, func() {
				if err := runConfigRemove(configRemoveCmd, []string{"dev"}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		})

		if !strings.Contains(output, "Removed 'dev'") {
			t.Errorf("should confirm removal, got: %q", output)
		}

		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		if _, exists := cfg.Servers["dev"]; exists {
			t.Error("dev should have been removed")
		}
		if cfg.Default != "prod" {
			t.Errorf("default should switch to prod, got %q", cfg.Default)
		}
	})

	t.Run("removes last connection", func(t *testing.T) {
		cfg := &config.Config{
			Default:  "only",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"only": {
					URL:      "https://server:8010/api/v1",
					User:     "admin",
					Password: config.EncodePassword("pass"),
					AuthMode: "basic",
				},
			},
		}
		setupTestHome(t, cfg)

		var output string
		withStdin(t, "y\n", func() {
			output = captureStdout(t, func() {
				if err := runConfigRemove(configRemoveCmd, []string{"only"}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		})

		if !strings.Contains(output, "No connections remaining") {
			t.Errorf("should say no remaining, got: %q", output)
		}
	})

	t.Run("aborts when user declines", func(t *testing.T) {
		setupTestHome(t, testConfig())

		withStdin(t, "n\n", func() {
			captureStdout(t, func() {
				if err := runConfigRemove(configRemoveCmd, []string{"prod"}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		})

		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		if _, exists := cfg.Servers["prod"]; !exists {
			t.Error("prod should still exist after declining")
		}
	})

	t.Run("error for non-existent connection", func(t *testing.T) {
		setupTestHome(t, testConfig())

		err := runConfigRemove(configRemoveCmd, []string{"nonexistent"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("error = %q, want 'not found'", err.Error())
		}
	})

	t.Run("error when no config", func(t *testing.T) {
		setupTestHome(t, nil)

		err := runConfigRemove(configRemoveCmd, []string{"something"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "No connection configured") {
			t.Errorf("error = %q, want 'No connection configured'", err.Error())
		}
	})
}

// --- config settings ---

func TestConfigSettings(t *testing.T) {
	t.Run("shows current settings as table", func(t *testing.T) {
		setupTestHome(t, testConfig())

		origFlag := flagOutput
		defer func() { flagOutput = origFlag }()
		flagOutput = ""

		output := captureStdout(t, func() {
			if err := runConfigSettings(configSettingsCmd, nil); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(output, "SETTING") {
			t.Errorf("should have header, got: %q", output)
		}
		if !strings.Contains(output, "limit") {
			t.Errorf("should show limit, got: %q", output)
		}
		if !strings.Contains(output, "50") {
			t.Errorf("should show default limit 50, got: %q", output)
		}
		if !strings.Contains(output, "table") {
			t.Errorf("should show output format 'table', got: %q", output)
		}
	})

	t.Run("shows settings as JSON", func(t *testing.T) {
		setupTestHome(t, testConfig())

		origFlag := flagOutput
		defer func() { flagOutput = origFlag }()
		flagOutput = "json"

		output := captureStdout(t, func() {
			if err := runConfigSettings(configSettingsCmd, nil); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		var result map[string]interface{}
		if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &result); err != nil {
			t.Fatalf("output is not valid JSON: %v\noutput: %q", err, output)
		}
		if result["default_limit"].(float64) != 50 {
			t.Errorf("default_limit = %v, want 50", result["default_limit"])
		}
		if result["output_format"].(string) != "table" {
			t.Errorf("output_format = %v, want 'table'", result["output_format"])
		}
	})

	t.Run("resets settings to defaults", func(t *testing.T) {
		cfg := testConfig()
		cfg.Settings.DefaultLimit = 200
		cfg.Settings.OutputFormat = "json"
		cfg.Settings.ShowSystem = true
		setupTestHome(t, cfg)

		origReset := settingsReset
		defer func() { settingsReset = origReset }()
		settingsReset = true

		output := captureStdout(t, func() {
			if err := runConfigSettings(configSettingsCmd, nil); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(output, "Settings reset to defaults") {
			t.Errorf("should confirm reset, got: %q", output)
		}

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		if saved.Settings.DefaultLimit != config.DefaultLimit {
			t.Errorf("DefaultLimit = %d, want %d", saved.Settings.DefaultLimit, config.DefaultLimit)
		}
		if saved.Settings.OutputFormat != config.DefaultOutput {
			t.Errorf("OutputFormat = %q, want %q", saved.Settings.OutputFormat, config.DefaultOutput)
		}
		if saved.Settings.ShowSystem != config.DefaultShowSystem {
			t.Errorf("ShowSystem = %v, want %v", saved.Settings.ShowSystem, config.DefaultShowSystem)
		}
	})

	t.Run("updates limit setting", func(t *testing.T) {
		setupTestHome(t, testConfig())

		// Mark the flag as changed via cobra's flag API
		configSettingsCmd.Flags().Set("limit", "100")
		defer func() {
			configSettingsCmd.Flags().Lookup("limit").Changed = false
			settingsLimit = 0
		}()

		output := captureStdout(t, func() {
			if err := runConfigSettings(configSettingsCmd, nil); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(output, "Settings updated") {
			t.Errorf("should confirm update, got: %q", output)
		}

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		if saved.Settings.DefaultLimit != 100 {
			t.Errorf("DefaultLimit = %d, want 100", saved.Settings.DefaultLimit)
		}
	})

	t.Run("updates output setting", func(t *testing.T) {
		setupTestHome(t, testConfig())

		configSettingsCmd.Flags().Set("output", "json")
		defer func() {
			configSettingsCmd.Flags().Lookup("output").Changed = false
			settingsOutput = ""
		}()

		output := captureStdout(t, func() {
			if err := runConfigSettings(configSettingsCmd, nil); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(output, "Settings updated") {
			t.Errorf("should confirm update, got: %q", output)
		}

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		if saved.Settings.OutputFormat != "json" {
			t.Errorf("OutputFormat = %q, want %q", saved.Settings.OutputFormat, "json")
		}
	})

	t.Run("creates config when none exists", func(t *testing.T) {
		setupTestHome(t, nil)

		origFlag := flagOutput
		defer func() { flagOutput = origFlag }()
		flagOutput = ""

		output := captureStdout(t, func() {
			if err := runConfigSettings(configSettingsCmd, nil); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(output, "SETTING") {
			t.Errorf("should still show settings table, got: %q", output)
		}
	})
}

// --- config edit (stub) ---

func TestConfigEdit(t *testing.T) {
	err := configEditCmd.RunE(configEditCmd, []string{"myserver"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("error = %q, want 'not yet implemented'", err.Error())
	}
}

// --- config add ---

func TestConfigAdd(t *testing.T) {
	t.Run("adds first connection with all flags", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[{"Name":"Sales"}]}`))
		}))
		defer ts.Close()

		setupTestHome(t, nil)
		defer saveAddFlags()()
		addFlagURL = ts.URL
		addFlagUser = "admin"
		addFlagPassword = "secret"
		addFlagAuth = "basic"
		addFlagNamespace = ""

		output := captureStdout(t, func() {
			if err := runConfigAdd(configAddCmd, []string{"myserver"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(output, "added and set as default") {
			t.Errorf("first server should be default, got: %q", output)
		}

		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		if cfg.Default != "myserver" {
			t.Errorf("default = %q, want 'myserver'", cfg.Default)
		}
		srv, ok := cfg.Servers["myserver"]
		if !ok {
			t.Fatal("myserver not in config")
		}
		if srv.URL != ts.URL {
			t.Errorf("URL = %q, want %q", srv.URL, ts.URL)
		}
		if srv.User != "admin" {
			t.Errorf("User = %q, want 'admin'", srv.User)
		}
		if srv.AuthMode != "basic" {
			t.Errorf("AuthMode = %q, want 'basic'", srv.AuthMode)
		}
	})

	t.Run("adds second connection without overwriting default", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		setupTestHome(t, testConfig())
		defer saveAddFlags()()
		addFlagURL = ts.URL
		addFlagUser = "user2"
		addFlagPassword = "pass2"
		addFlagAuth = "basic"

		output := captureStdout(t, func() {
			if err := runConfigAdd(configAddCmd, []string{"staging"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(output, "Connection 'staging' added.") {
			t.Errorf("should say added (not as default), got: %q", output)
		}

		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		if cfg.Default != "dev" {
			t.Errorf("default should still be 'dev', got %q", cfg.Default)
		}
	})

	t.Run("rejects duplicate name", func(t *testing.T) {
		setupTestHome(t, testConfig())

		output := captureStdout(t, func() {
			err := runConfigAdd(configAddCmd, []string{"dev"})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(output, "already exists") {
			t.Errorf("should say already exists, got: %q", output)
		}
	})

	t.Run("error for empty name from stdin", func(t *testing.T) {
		setupTestHome(t, nil)

		var err error
		withStdin(t, "\n", func() {
			err = runConfigAdd(configAddCmd, nil)
		})

		if err == nil {
			t.Fatal("expected error for empty name")
		}
		if !strings.Contains(err.Error(), "connection name is required") {
			t.Errorf("error = %q, want 'connection name is required'", err.Error())
		}
	})

	t.Run("error for invalid auth mode", func(t *testing.T) {
		setupTestHome(t, nil)
		defer saveAddFlags()()
		addFlagURL = "https://server:8010/api/v1"
		addFlagUser = "admin"
		addFlagPassword = "secret"
		addFlagAuth = "invalid"

		err := runConfigAdd(configAddCmd, []string{"test"})
		if err == nil {
			t.Fatal("expected error for invalid auth")
		}
		if !strings.Contains(err.Error(), "auth mode must be") {
			t.Errorf("error = %q, want 'auth mode must be'", err.Error())
		}
	})

	t.Run("adds CAM connection with namespace", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		setupTestHome(t, nil)
		defer saveAddFlags()()
		addFlagURL = ts.URL
		addFlagUser = "admin"
		addFlagPassword = "secret"
		addFlagAuth = "cam"
		addFlagNamespace = "LDAP"

		captureStdout(t, func() {
			if err := runConfigAdd(configAddCmd, []string{"cam-server"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		srv := cfg.Servers["cam-server"]
		if srv.AuthMode != "cam" {
			t.Errorf("AuthMode = %q, want 'cam'", srv.AuthMode)
		}
		if srv.Namespace != "LDAP" {
			t.Errorf("Namespace = %q, want 'LDAP'", srv.Namespace)
		}
	})

	t.Run("saves with warning on connection failure when user confirms", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		closedURL := ts.URL
		ts.Close()

		setupTestHome(t, nil)
		defer saveAddFlags()()
		addFlagURL = closedURL
		addFlagUser = "admin"
		addFlagPassword = "secret"
		addFlagAuth = "basic"

		var output string
		withStdin(t, "y\n", func() {
			output = captureStdout(t, func() {
				if err := runConfigAdd(configAddCmd, []string{"failserver"}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		})

		if !strings.Contains(output, "added and set as default") {
			t.Errorf("should be saved, got: %q", output)
		}

		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		if _, ok := cfg.Servers["failserver"]; !ok {
			t.Error("failserver should be saved despite connection failure")
		}
	})

	t.Run("does not save on connection failure when user declines", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		closedURL := ts.URL
		ts.Close()

		setupTestHome(t, nil)
		defer saveAddFlags()()
		addFlagURL = closedURL
		addFlagUser = "admin"
		addFlagPassword = "secret"
		addFlagAuth = "basic"

		withStdin(t, "n\n", func() {
			captureStdout(t, func() {
				if err := runConfigAdd(configAddCmd, []string{"nosave"}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		})

		cfg, _ := config.Load()
		if cfg != nil {
			if _, ok := cfg.Servers["nosave"]; ok {
				t.Error("nosave should not be saved after declining")
			}
		}
	})

	t.Run("password stored base64-encoded", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		setupTestHome(t, nil)
		defer saveAddFlags()()
		addFlagURL = ts.URL
		addFlagUser = "admin"
		addFlagPassword = "my$ecret!123"
		addFlagAuth = "basic"

		captureStdout(t, func() {
			if err := runConfigAdd(configAddCmd, []string{"pw-test"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		srv := cfg.Servers["pw-test"]
		decoded, err := config.DecodePassword(srv.Password)
		if err != nil {
			t.Fatalf("cannot decode password: %v", err)
		}
		if decoded != "my$ecret!123" {
			t.Errorf("decoded password = %q, want %q", decoded, "my$ecret!123")
		}
	})

	t.Run("uses TM1CLI_PASSWORD env var", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		setupTestHome(t, nil)
		t.Setenv("TM1CLI_PASSWORD", "env-password")
		defer saveAddFlags()()
		addFlagURL = ts.URL
		addFlagUser = "admin"
		addFlagPassword = "" // empty — should use env var
		addFlagAuth = "basic"

		captureStdout(t, func() {
			if err := runConfigAdd(configAddCmd, []string{"env-test"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		srv := cfg.Servers["env-test"]
		decoded, err := config.DecodePassword(srv.Password)
		if err != nil {
			t.Fatalf("cannot decode password: %v", err)
		}
		if decoded != "env-password" {
			t.Errorf("decoded password = %q, want %q", decoded, "env-password")
		}
	})
}

// --- promptYesNo ---

func TestPromptYesNo(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"y returns true", "y\n", true},
		{"yes returns true", "yes\n", true},
		{"Y returns true (case insensitive)", "Y\n", true},
		{"YES returns true (case insensitive)", "YES\n", true},
		{"n returns false", "n\n", false},
		{"no returns false", "no\n", false},
		{"empty returns false", "\n", false},
		{"random text returns false", "maybe\n", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			// Capture the prompt text written to stdout
			captureStdout(t, func() {
				got := promptYesNo(reader, "Test?")
				if got != tt.want {
					t.Errorf("promptYesNo(%q) = %v, want %v", tt.input, got, tt.want)
				}
			})
		})
	}
}

// --- createClientFromServerConfig ---

func TestCreateClientFromServerConfig(t *testing.T) {
	srv := config.ServerConfig{
		URL:      "https://localhost:8010/api/v1",
		User:     "admin",
		AuthMode: "basic",
	}
	c, err := createClientFromServerConfig(srv, "password", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c == nil {
		t.Error("client should not be nil")
	}
}
