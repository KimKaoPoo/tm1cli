package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"tm1cli/internal/config"

	"github.com/zalando/go-keyring"
)

// setupTestHome creates a temp directory, sets HOME, and optionally writes a config file.
func setupTestHome(t *testing.T, cfg *config.Config) {
	t.Helper()
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("TM1CLI_CONFIG", "")
	t.Chdir(tmpDir)

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
		if !strings.Contains(output, "Config:") {
			t.Errorf("should show config source, got: %q", output)
		}
		if !strings.Contains(output, "(global)") {
			t.Errorf("should show global source, got: %q", output)
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
		if !strings.Contains(output, "Run 'tm1cli config add' first") {
			t.Errorf("should say run config add first, got: %q", output)
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

// --- config edit ---

// withMockPassword overrides readPasswordFn for the duration of fn,
// making it return the given password string (empty string simulates Enter).
func withMockPassword(t *testing.T, pw string, fn func()) {
	t.Helper()
	orig := readPasswordFn
	readPasswordFn = func() (string, error) {
		return pw, nil
	}
	defer func() { readPasswordFn = orig }()
	fn()
}

func TestConfigEdit(t *testing.T) {
	t.Run("keeps all values when pressing Enter through all prompts", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[{"Name":"Sales"}]}`))
		}))
		defer ts.Close()

		cfg := &config.Config{
			Default:  "myserver",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"myserver": {
					URL:      ts.URL + "/api/v1",
					User:     "admin",
					Password: config.EncodePassword("secret"),
					AuthMode: "basic",
				},
			},
		}
		setupTestHome(t, cfg)

		// All Enter = keep existing values
		var output string
		withStdin(t, "\n\n\n\n", func() {
			withMockPassword(t, "", func() {
				output = captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"myserver"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		if !strings.Contains(output, "updated") {
			t.Errorf("should confirm update, got: %q", output)
		}

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		srv := saved.Servers["myserver"]
		if srv.URL != ts.URL+"/api/v1" {
			t.Errorf("URL = %q, want %q", srv.URL, ts.URL+"/api/v1")
		}
		if srv.User != "admin" {
			t.Errorf("User = %q, want 'admin'", srv.User)
		}
		if srv.AuthMode != "basic" {
			t.Errorf("AuthMode = %q, want 'basic'", srv.AuthMode)
		}
		decoded, _ := config.DecodePassword(srv.Password)
		if decoded != "secret" {
			t.Errorf("password = %q, want 'secret'", decoded)
		}
	})

	t.Run("changes URL only", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		cfg := &config.Config{
			Default:  "myserver",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"myserver": {
					URL:      "https://old-server:8010/api/v1",
					User:     "admin",
					Password: config.EncodePassword("secret"),
					AuthMode: "basic",
				},
			},
		}
		setupTestHome(t, cfg)

		// New URL, then Enter for rest
		input := ts.URL + "/api/v1\n\n\n\n"
		withStdin(t, input, func() {
			withMockPassword(t, "", func() {
				captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"myserver"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		srv := saved.Servers["myserver"]
		if srv.URL != ts.URL+"/api/v1" {
			t.Errorf("URL = %q, want %q", srv.URL, ts.URL+"/api/v1")
		}
		if srv.User != "admin" {
			t.Errorf("User should be unchanged, got %q", srv.User)
		}
	})

	t.Run("changes auth mode from basic to cam and prompts namespace", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		cfg := &config.Config{
			Default:  "myserver",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"myserver": {
					URL:      ts.URL + "/api/v1",
					User:     "admin",
					Password: config.EncodePassword("secret"),
					AuthMode: "basic",
				},
			},
		}
		setupTestHome(t, cfg)

		// Enter URL, "cam" for auth, "LDAP" for namespace, Enter for user
		input := "\ncam\nLDAP\n\n"
		withStdin(t, input, func() {
			withMockPassword(t, "", func() {
				captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"myserver"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		srv := saved.Servers["myserver"]
		if srv.AuthMode != "cam" {
			t.Errorf("AuthMode = %q, want 'cam'", srv.AuthMode)
		}
		if srv.Namespace != "LDAP" {
			t.Errorf("Namespace = %q, want 'LDAP'", srv.Namespace)
		}
	})

	t.Run("changes auth mode from cam to basic and clears namespace", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		cfg := &config.Config{
			Default:  "myserver",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"myserver": {
					URL:       ts.URL + "/api/v1",
					User:      "admin",
					Password:  config.EncodePassword("secret"),
					AuthMode:  "cam",
					Namespace: "LDAP",
				},
			},
		}
		setupTestHome(t, cfg)

		// Enter URL, "basic" for auth, Enter for user (no namespace prompt)
		input := "\nbasic\n\n"
		withStdin(t, input, func() {
			withMockPassword(t, "", func() {
				captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"myserver"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		srv := saved.Servers["myserver"]
		if srv.AuthMode != "basic" {
			t.Errorf("AuthMode = %q, want 'basic'", srv.AuthMode)
		}
		if srv.Namespace != "" {
			t.Errorf("Namespace = %q, want empty (cleared)", srv.Namespace)
		}
	})

	t.Run("changes password", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		cfg := &config.Config{
			Default:  "myserver",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"myserver": {
					URL:      ts.URL + "/api/v1",
					User:     "admin",
					Password: config.EncodePassword("oldpass"),
					AuthMode: "basic",
				},
			},
		}
		setupTestHome(t, cfg)

		// Enter for all fields except password
		withStdin(t, "\n\n\n", func() {
			withMockPassword(t, "newpass", func() {
				captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"myserver"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		got, err := saved.GetEffectivePassword("myserver")
		if err != nil {
			t.Fatalf("cannot retrieve effective password: %v", err)
		}
		if got != "newpass" {
			t.Errorf("password = %q, want 'newpass'", got)
		}
	})

	t.Run("error for non-existent connection", func(t *testing.T) {
		setupTestHome(t, testConfig())

		err := runConfigEdit(configEditCmd, []string{"nonexistent"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("error = %q, want 'not found'", err.Error())
		}
	})

	t.Run("error when no config exists", func(t *testing.T) {
		setupTestHome(t, nil)

		err := runConfigEdit(configEditCmd, []string{"myserver"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "No connection configured") {
			t.Errorf("error = %q, want 'No connection configured'", err.Error())
		}
	})

	t.Run("error for invalid auth mode", func(t *testing.T) {
		setupTestHome(t, testConfig())

		// Enter URL, then invalid auth mode
		withStdin(t, "\ninvalid\n", func() {
			err := runConfigEdit(configEditCmd, []string{"dev"})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "auth mode must be") {
				t.Errorf("error = %q, want 'auth mode must be'", err.Error())
			}
		})
	})

	t.Run("connection test success shows checkmark", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[{"Name":"Sales"}]}`))
		}))
		defer ts.Close()

		cfg := &config.Config{
			Default:  "myserver",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"myserver": {
					URL:      ts.URL + "/api/v1",
					User:     "admin",
					Password: config.EncodePassword("secret"),
					AuthMode: "basic",
				},
			},
		}
		setupTestHome(t, cfg)

		var output string
		withStdin(t, "\n\n\n\n", func() {
			withMockPassword(t, "", func() {
				output = captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"myserver"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		if !strings.Contains(output, "Testing connection...") {
			t.Errorf("should show testing message, got: %q", output)
		}
	})

	t.Run("connection test failure with save anyway", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		closedURL := ts.URL
		ts.Close()

		cfg := &config.Config{
			Default:  "myserver",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"myserver": {
					URL:      closedURL + "/api/v1",
					User:     "admin",
					Password: config.EncodePassword("secret"),
					AuthMode: "basic",
				},
			},
		}
		setupTestHome(t, cfg)

		// Enter through fields, then "y" for save anyway
		var output string
		withStdin(t, "\n\n\ny\n", func() {
			withMockPassword(t, "", func() {
				output = captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"myserver"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		if !strings.Contains(output, "updated") {
			t.Errorf("should save anyway, got: %q", output)
		}

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		if _, ok := saved.Servers["myserver"]; !ok {
			t.Error("myserver should still be saved")
		}
	})

	t.Run("connection test failure and user declines save", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		closedURL := ts.URL
		ts.Close()

		cfg := &config.Config{
			Default:  "myserver",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"myserver": {
					URL:      "https://original-server:8010/api/v1",
					User:     "admin",
					Password: config.EncodePassword("secret"),
					AuthMode: "basic",
				},
			},
		}
		setupTestHome(t, cfg)

		// Change URL to closed server, then "n" for save anyway
		withStdin(t, closedURL+"/api/v1\n\n\nn\n", func() {
			withMockPassword(t, "", func() {
				captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"myserver"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		srv := saved.Servers["myserver"]
		if srv.URL != "https://original-server:8010/api/v1" {
			t.Errorf("URL should be unchanged after decline, got %q", srv.URL)
		}
	})

	t.Run("default connection remains default after edit", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		cfg := testConfig()
		cfg.Servers["dev"] = config.ServerConfig{
			URL:      ts.URL + "/api/v1",
			User:     "admin",
			Password: config.EncodePassword("secret"),
			AuthMode: "basic",
		}
		setupTestHome(t, cfg)

		withStdin(t, "\n\n\n\n", func() {
			withMockPassword(t, "", func() {
				captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"dev"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		if saved.Default != "dev" {
			t.Errorf("default should still be 'dev', got %q", saved.Default)
		}
	})

	t.Run("shows current values in prompts", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		cfg := &config.Config{
			Default:  "myserver",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"myserver": {
					URL:      ts.URL + "/api/v1",
					User:     "testuser",
					Password: config.EncodePassword("secret"),
					AuthMode: "basic",
				},
			},
		}
		setupTestHome(t, cfg)

		var output string
		withStdin(t, "\n\n\n\n", func() {
			withMockPassword(t, "", func() {
				output = captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"myserver"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		if !strings.Contains(output, "Editing connection 'myserver'") {
			t.Errorf("should show editing header, got: %q", output)
		}
		if !strings.Contains(output, ts.URL+"/api/v1") {
			t.Errorf("should show current URL in prompt, got: %q", output)
		}
		if !strings.Contains(output, "[basic]") {
			t.Errorf("should show current auth mode in prompt, got: %q", output)
		}
		if !strings.Contains(output, "[testuser]") {
			t.Errorf("should show current username in prompt, got: %q", output)
		}
		if !strings.Contains(output, "[****]") {
			t.Errorf("should show masked password, got: %q", output)
		}
	})

	t.Run("edits CAM connection keeping namespace", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		cfg := &config.Config{
			Default:  "myserver",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"myserver": {
					URL:       ts.URL + "/api/v1",
					User:      "admin",
					Password:  config.EncodePassword("secret"),
					AuthMode:  "cam",
					Namespace: "LDAP",
				},
			},
		}
		setupTestHome(t, cfg)

		// Enter through all fields (keep cam, keep LDAP namespace)
		withStdin(t, "\n\n\n\n", func() {
			withMockPassword(t, "", func() {
				captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"myserver"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		srv := saved.Servers["myserver"]
		if srv.AuthMode != "cam" {
			t.Errorf("AuthMode = %q, want 'cam'", srv.AuthMode)
		}
		if srv.Namespace != "LDAP" {
			t.Errorf("Namespace = %q, want 'LDAP'", srv.Namespace)
		}
	})

	t.Run("uses TM1CLI_PASSWORD env var when Enter pressed for password", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		cfg := &config.Config{
			Default:  "myserver",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"myserver": {
					URL:      ts.URL + "/api/v1",
					User:     "admin",
					Password: config.EncodePassword("oldpass"),
					AuthMode: "basic",
				},
			},
		}
		setupTestHome(t, cfg)
		t.Setenv("TM1CLI_PASSWORD", "env-password")

		// Enter through all fields, empty password = use env var
		withStdin(t, "\n\n\n\n", func() {
			withMockPassword(t, "", func() {
				captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"myserver"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		// Password in config should still be the old stored one (env var only used for connection test)
		decoded, _ := config.DecodePassword(saved.Servers["myserver"].Password)
		if decoded != "oldpass" {
			t.Errorf("stored password = %q, want 'oldpass' (env var should not change stored value)", decoded)
		}
	})
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

	t.Run("password stored and retrievable", func(t *testing.T) {
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
		got, err := cfg.GetEffectivePassword("pw-test")
		if err != nil {
			t.Fatalf("cannot retrieve effective password: %v", err)
		}
		if got != "my$ecret!123" {
			t.Errorf("password = %q, want %q", got, "my$ecret!123")
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
		// Env var is used at add-time for the password value stored in keychain/base64.
		// Since TM1CLI_PASSWORD is still set here, GetEffectivePassword returns the env
		// var directly, which matches. Unset the env var to verify the stored value.
		os.Unsetenv("TM1CLI_PASSWORD")
		got, err := cfg.GetEffectivePassword("env-test")
		if err != nil {
			t.Fatalf("cannot retrieve effective password: %v", err)
		}
		if got != "env-password" {
			t.Errorf("password = %q, want %q", got, "env-password")
		}
	})
}

// --- keychain-specific integration tests ---

func TestConfigAddKeychain(t *testing.T) {
	t.Run("writes to keychain when available", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		setupTestHome(t, nil)
		defer saveAddFlags()()
		addFlagURL = ts.URL
		addFlagUser = "admin"
		addFlagPassword = "kc-secret"
		addFlagAuth = "basic"

		captureStdout(t, func() {
			if err := runConfigAdd(configAddCmd, []string{"kc-test"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		srv := cfg.Servers["kc-test"]
		if srv.PasswordStorage != config.PasswordStorageKeychain {
			t.Errorf("PasswordStorage = %q, want %q", srv.PasswordStorage, config.PasswordStorageKeychain)
		}
		if srv.Password != "" {
			t.Errorf("Password field should be empty when keychain is used, got %q", srv.Password)
		}
		if srv.PasswordRef == "" {
			t.Error("PasswordRef should be set when keychain is used")
		}
		kcValue, err := config.GetKeychainPassword(srv.PasswordRef)
		if err != nil || kcValue != "kc-secret" {
			t.Errorf("keychain value = %q, %v; want 'kc-secret', nil", kcValue, err)
		}
	})

	t.Run("falls back to base64 when keychain unavailable", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		restore := config.OverrideKeychainSet(func(service, user, password string) error {
			return fmt.Errorf("simulated keychain failure")
		})
		defer restore()

		setupTestHome(t, nil)
		defer saveAddFlags()()
		addFlagURL = ts.URL
		addFlagUser = "admin"
		addFlagPassword = "fallback-secret"
		addFlagAuth = "basic"

		captured := captureAll(t, func() {
			if err := runConfigAdd(configAddCmd, []string{"fb-test"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(captured.Stderr, "keychain unavailable") {
			t.Errorf("stderr should warn about keychain failure, got: %q", captured.Stderr)
		}

		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		srv := cfg.Servers["fb-test"]
		if srv.PasswordStorage != config.PasswordStorageBase64 {
			t.Errorf("PasswordStorage = %q, want %q", srv.PasswordStorage, config.PasswordStorageBase64)
		}
		if srv.PasswordRef != "" {
			t.Errorf("PasswordRef should be empty on base64 fallback, got %q", srv.PasswordRef)
		}
		decoded, err := config.DecodePassword(srv.Password)
		if err != nil || decoded != "fallback-secret" {
			t.Errorf("decoded password = %q, %v; want 'fallback-secret', nil", decoded, err)
		}
	})

	t.Run("rolls back keychain entry when config save fails", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		// Capture refs passed to keychainSet so we can verify cleanup.
		var capturedRef string
		restoreSet := config.OverrideKeychainSet(func(service, user, password string) error {
			capturedRef = user
			return nil // keychain write succeeds
		})
		defer restoreSet()

		var deletedRef string
		restoreDel := config.OverrideKeychainDelete(func(service, user string) error {
			deletedRef = user
			return nil
		})
		defer restoreDel()

		// Setup: seed an empty config file, then make the config dir
		// read-only so Save's overwrite fails while Load still succeeds.
		// This exercises the rollback branch in runConfigAdd where the
		// keychain write succeeds but the subsequent Save call errors.
		tmpDir := t.TempDir()
		t.Setenv("HOME", tmpDir)
		t.Setenv("TM1CLI_CONFIG", "")
		t.Chdir(tmpDir)

		cfgDir := filepath.Join(tmpDir, ".tm1cli")
		if err := os.MkdirAll(cfgDir, 0700); err != nil {
			t.Fatalf("setup mkdir: %v", err)
		}
		cfgPath := filepath.Join(cfgDir, "config.json")
		if err := os.WriteFile(cfgPath,
			[]byte(`{"default":"","servers":{},"settings":{"default_limit":50,"output_format":"table"}}`),
			0600); err != nil {
			t.Fatalf("setup write: %v", err)
		}
		// Make the config file read-only so Save's truncate-write fails.
		if err := os.Chmod(cfgPath, 0400); err != nil {
			t.Fatalf("setup chmod: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(cfgPath, 0600) })

		defer saveAddFlags()()
		addFlagURL = ts.URL
		addFlagUser = "admin"
		addFlagPassword = "rollback-secret"
		addFlagAuth = "basic"

		err := runConfigAdd(configAddCmd, []string{"rb-test"})
		if err == nil {
			t.Fatal("expected save to fail, got nil error")
		}
		if capturedRef == "" {
			t.Fatal("keychain write was never attempted")
		}
		if deletedRef != capturedRef {
			t.Errorf("rollback ref = %q, want %q (same as written)", deletedRef, capturedRef)
		}
	})
}

func TestConfigEditKeychain(t *testing.T) {
	t.Run("new password migrates base64 to keychain", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		cfg := &config.Config{
			Default:  "mig",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"mig": {
					URL:      ts.URL + "/api/v1",
					User:     "admin",
					Password: config.EncodePassword("oldpass"),
					AuthMode: "basic",
				},
			},
		}
		setupTestHome(t, cfg)

		withStdin(t, "\n\n\n", func() {
			withMockPassword(t, "newpass", func() {
				captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"mig"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		srv := saved.Servers["mig"]
		if srv.PasswordStorage != config.PasswordStorageKeychain {
			t.Errorf("PasswordStorage = %q, want %q (migration to keychain)", srv.PasswordStorage, config.PasswordStorageKeychain)
		}
		kcValue, err := config.GetKeychainPassword(srv.PasswordRef)
		if err != nil || kcValue != "newpass" {
			t.Errorf("keychain value = %q, %v; want 'newpass', nil", kcValue, err)
		}
	})

	t.Run("blank password preserves base64 storage", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		cfg := &config.Config{
			Default:  "keep",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"keep": {
					URL:      ts.URL + "/api/v1",
					User:     "admin",
					Password: config.EncodePassword("oldpass"),
					AuthMode: "basic",
					// PasswordStorage empty (legacy base64)
				},
			},
		}
		setupTestHome(t, cfg)

		withStdin(t, "\n\n\n", func() {
			withMockPassword(t, "", func() {
				captureStdout(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"keep"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		srv := saved.Servers["keep"]
		if srv.PasswordStorage != "" {
			t.Errorf("PasswordStorage = %q, want empty (preserved legacy)", srv.PasswordStorage)
		}
		if srv.PasswordRef != "" {
			t.Errorf("PasswordRef = %q, want empty (preserved legacy)", srv.PasswordRef)
		}
		decoded, err := config.DecodePassword(srv.Password)
		if err != nil || decoded != "oldpass" {
			t.Errorf("decoded password = %q, %v; want 'oldpass', nil", decoded, err)
		}
	})

	t.Run("connection test failure with decline keeps old keychain password", func(t *testing.T) {
		// Arrange a keychain-backed server and a mock TM1 that fails the test.
		ref := "edit-testfail-ref"
		if err := config.SetKeychainPassword(ref, "oldkc"); err != nil {
			t.Fatalf("setup: %v", err)
		}
		defer config.DeleteKeychainPassword(ref)

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer ts.Close()

		cfg := &config.Config{
			Default:  "kc",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"kc": {
					URL:             ts.URL + "/api/v1",
					User:            "admin",
					PasswordStorage: config.PasswordStorageKeychain,
					PasswordRef:     ref,
					AuthMode:        "basic",
				},
			},
		}
		setupTestHome(t, cfg)

		// Enter for URL/auth/user, new password "newpw", then 'n' to "Save anyway?".
		withStdin(t, "\n\n\nn\n", func() {
			withMockPassword(t, "newpw", func() {
				captureAll(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"kc"}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				})
			})
		})

		// The old keychain password must still be intact.
		got, err := config.GetKeychainPassword(ref)
		if err != nil {
			t.Fatalf("keychain lookup failed: %v", err)
		}
		if got != "oldkc" {
			t.Errorf("keychain value = %q, want 'oldkc' (edit was declined, must not mutate)", got)
		}
	})

	t.Run("save failure restores previous keychain password", func(t *testing.T) {
		// Arrange a keychain-backed server.
		ref := "edit-savefail-ref"
		if err := config.SetKeychainPassword(ref, "prevkc"); err != nil {
			t.Fatalf("setup: %v", err)
		}
		defer config.DeleteKeychainPassword(ref)

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		tmpDir := t.TempDir()
		t.Setenv("HOME", tmpDir)
		t.Setenv("TM1CLI_CONFIG", "")
		t.Chdir(tmpDir)
		cfgDir := filepath.Join(tmpDir, ".tm1cli")
		if err := os.MkdirAll(cfgDir, 0700); err != nil {
			t.Fatalf("setup mkdir: %v", err)
		}
		cfgPath := filepath.Join(cfgDir, "config.json")
		cfgBody := `{"default":"kc","servers":{"kc":{"url":"` + ts.URL + `/api/v1","user":"admin","password":"","password_storage":"keychain","password_ref":"` + ref + `","auth_mode":"basic"}},"settings":{"default_limit":50,"output_format":"table"}}`
		if err := os.WriteFile(cfgPath, []byte(cfgBody), 0600); err != nil {
			t.Fatalf("setup write: %v", err)
		}
		if err := os.Chmod(cfgPath, 0400); err != nil {
			t.Fatalf("setup chmod: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(cfgPath, 0600) })

		withStdin(t, "\n\n\n", func() {
			withMockPassword(t, "newkc", func() {
				captureAll(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"kc"}); err == nil {
						t.Fatal("expected Save to fail, got nil")
					}
				})
			})
		})

		// After the failed save, the keychain should still hold the old value.
		got, err := config.GetKeychainPassword(ref)
		if err != nil {
			t.Fatalf("keychain lookup failed: %v", err)
		}
		if got != "prevkc" {
			t.Errorf("keychain value = %q, want 'prevkc' (save failed, restore required)", got)
		}
	})

	t.Run("keychain write fails then save fails restores original keychain", func(t *testing.T) {
		// Arrange a keychain-backed server with a live secret.
		ref := "edit-compound-ref"
		if err := config.SetKeychainPassword(ref, "origkc"); err != nil {
			t.Fatalf("setup: %v", err)
		}
		defer config.DeleteKeychainPassword(ref)

		// Simulate a transient keychain failure: the StorePassword write
		// fails (causing fallback + deletion of the old entry), but a
		// subsequent restore write succeeds. Without this, the test would
		// only exercise the true-unavailable path where no recovery is
		// possible — the rollback logic we're testing requires a working
		// keychain by the time it runs.
		realSet := keyring.Set
		var setCallCount int
		restoreSet := config.OverrideKeychainSet(func(service, user, password string) error {
			setCallCount++
			if setCallCount == 1 {
				return fmt.Errorf("simulated transient keychain failure")
			}
			return realSet(service, user, password)
		})
		defer restoreSet()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":[]}`))
		}))
		defer ts.Close()

		tmpDir := t.TempDir()
		t.Setenv("HOME", tmpDir)
		t.Setenv("TM1CLI_CONFIG", "")
		t.Chdir(tmpDir)
		cfgDir := filepath.Join(tmpDir, ".tm1cli")
		if err := os.MkdirAll(cfgDir, 0700); err != nil {
			t.Fatalf("setup mkdir: %v", err)
		}
		cfgPath := filepath.Join(cfgDir, "config.json")
		cfgBody := `{"default":"kc","servers":{"kc":{"url":"` + ts.URL + `/api/v1","user":"admin","password":"","password_storage":"keychain","password_ref":"` + ref + `","auth_mode":"basic"}},"settings":{"default_limit":50,"output_format":"table"}}`
		if err := os.WriteFile(cfgPath, []byte(cfgBody), 0600); err != nil {
			t.Fatalf("setup write: %v", err)
		}
		// Make the file read-only so Save fails.
		if err := os.Chmod(cfgPath, 0400); err != nil {
			t.Fatalf("setup chmod: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(cfgPath, 0600) })

		withStdin(t, "\n\n\n", func() {
			withMockPassword(t, "brandnew", func() {
				captureAll(t, func() {
					if err := runConfigEdit(configEditCmd, []string{"kc"}); err == nil {
						t.Fatal("expected Save to fail, got nil")
					}
				})
			})
		})

		// The original keychain entry must be restored — the on-disk config
		// still points at this ref, so it has to resolve.
		got, err := config.GetKeychainPassword(ref)
		if err != nil {
			t.Fatalf("keychain lookup failed: %v — original entry was deleted and not restored", err)
		}
		if got != "origkc" {
			t.Errorf("keychain value = %q, want 'origkc' (compound failure must restore)", got)
		}
	})
}

func TestConfigRemoveKeychain(t *testing.T) {
	t.Run("deletes keychain entry when removing", func(t *testing.T) {
		ref := "test-remove-ref"
		if err := config.SetKeychainPassword(ref, "stored-secret"); err != nil {
			t.Fatalf("setup: %v", err)
		}
		defer config.DeleteKeychainPassword(ref)

		cfg := &config.Config{
			Default:  "rm",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"rm": {
					URL:             "https://host:8010/api/v1",
					User:            "admin",
					PasswordStorage: config.PasswordStorageKeychain,
					PasswordRef:     ref,
					AuthMode:        "basic",
				},
			},
		}
		setupTestHome(t, cfg)

		withStdin(t, "y\n", func() {
			captureStdout(t, func() {
				if err := runConfigRemove(configRemoveCmd, []string{"rm"}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		})

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config: %v", err)
		}
		if _, ok := saved.Servers["rm"]; ok {
			t.Error("server 'rm' should be removed from config")
		}

		_, err = config.GetKeychainPassword(ref)
		if err == nil {
			t.Error("keychain entry should have been deleted")
		}
	})

	t.Run("keychain delete failure does not break config save", func(t *testing.T) {
		restore := config.OverrideKeychainDelete(func(service, user string) error {
			return fmt.Errorf("simulated delete failure")
		})
		defer restore()

		cfg := &config.Config{
			Default:  "rm-fail",
			Settings: config.DefaultSettings(),
			Servers: map[string]config.ServerConfig{
				"rm-fail": {
					URL:             "https://host:8010/api/v1",
					User:            "admin",
					PasswordStorage: config.PasswordStorageKeychain,
					PasswordRef:     "some-ref",
					AuthMode:        "basic",
				},
			},
		}
		setupTestHome(t, cfg)

		captured := captureAll(t, func() {
			withStdin(t, "y\n", func() {
				if err := runConfigRemove(configRemoveCmd, []string{"rm-fail"}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		})

		if !strings.Contains(captured.Stderr, "orphaned") {
			t.Errorf("stderr should warn about orphaned keychain entry, got: %q", captured.Stderr)
		}

		saved, err := config.Load()
		if err != nil {
			t.Fatalf("cannot load config after remove: %v", err)
		}
		if _, ok := saved.Servers["rm-fail"]; ok {
			t.Error("server 'rm-fail' should be removed from config despite keychain failure")
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
