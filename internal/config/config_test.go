package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/zalando/go-keyring"
)

// setTestHome overrides HOME so that globalConfigDir() and globalConfigPath() point to a temp directory,
// and changes cwd to isolate findLocalConfig() from any real project directory.
// Returns the path to the config dir (~/.tm1cli) within that temp home.
func setTestHome(t *testing.T) string {
	t.Helper()
	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)
	t.Setenv("TM1CLI_CONFIG", "")
	t.Chdir(tmpHome)
	dir := filepath.Join(tmpHome, ".tm1cli")
	return dir
}

// globalPathForTest returns the global config path for use in findLocalConfig tests.
func globalPathForTest(t *testing.T) string {
	t.Helper()
	gp, err := globalConfigPath()
	if err != nil {
		t.Fatalf("globalConfigPath failed: %v", err)
	}
	return gp
}

// writeConfigJSON writes raw JSON content to the config file path for testing Load().
func writeConfigJSON(t *testing.T, dir string, content string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatalf("cannot create config dir: %v", err)
	}
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("cannot write config file: %v", err)
	}
}

func TestDefaultSettings(t *testing.T) {
	s := DefaultSettings()

	if s.DefaultLimit != DefaultLimit {
		t.Errorf("DefaultLimit = %d, want %d", s.DefaultLimit, DefaultLimit)
	}
	if s.OutputFormat != DefaultOutput {
		t.Errorf("OutputFormat = %q, want %q", s.OutputFormat, DefaultOutput)
	}
	if s.ShowSystem != DefaultShowSystem {
		t.Errorf("ShowSystem = %v, want %v", s.ShowSystem, DefaultShowSystem)
	}
	if s.TLSVerify != DefaultTLSVerify {
		t.Errorf("TLSVerify = %v, want %v", s.TLSVerify, DefaultTLSVerify)
	}
}

func TestNewConfig(t *testing.T) {
	cfg := NewConfig()

	if cfg.Default != "" {
		t.Errorf("Default = %q, want empty string", cfg.Default)
	}
	if cfg.Servers == nil {
		t.Fatal("Servers map should not be nil")
	}
	if len(cfg.Servers) != 0 {
		t.Errorf("Servers length = %d, want 0", len(cfg.Servers))
	}
	if cfg.Settings.DefaultLimit != DefaultLimit {
		t.Errorf("Settings.DefaultLimit = %d, want %d", cfg.Settings.DefaultLimit, DefaultLimit)
	}
	if cfg.Settings.OutputFormat != DefaultOutput {
		t.Errorf("Settings.OutputFormat = %q, want %q", cfg.Settings.OutputFormat, DefaultOutput)
	}
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(t *testing.T, dir string)
		wantNil     bool
		wantErr     bool
		errContains string
		validate    func(t *testing.T, cfg *Config)
	}{
		{
			name:    "missing file returns nil config and nil error",
			setup:   func(t *testing.T, dir string) {},
			wantNil: true,
			wantErr: false,
		},
		{
			name: "valid JSON loads correctly",
			setup: func(t *testing.T, dir string) {
				cfg := &Config{
					Default: "dev",
					Settings: Settings{
						DefaultLimit: 100,
						OutputFormat: "json",
						ShowSystem:   true,
						TLSVerify:    true,
					},
					Servers: map[string]ServerConfig{
						"dev": {
							URL:      "https://localhost:8010/api/v1",
							User:     "admin",
							Password: EncodePassword("secret"),
							AuthMode: "basic",
						},
					},
				}
				data, _ := json.MarshalIndent(cfg, "", "  ")
				writeConfigJSON(t, dir, string(data))
			},
			wantNil: false,
			wantErr: false,
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Default != "dev" {
					t.Errorf("Default = %q, want %q", cfg.Default, "dev")
				}
				if cfg.Settings.DefaultLimit != 100 {
					t.Errorf("DefaultLimit = %d, want 100", cfg.Settings.DefaultLimit)
				}
				if cfg.Settings.OutputFormat != "json" {
					t.Errorf("OutputFormat = %q, want %q", cfg.Settings.OutputFormat, "json")
				}
				srv, ok := cfg.Servers["dev"]
				if !ok {
					t.Fatal("server 'dev' not found")
				}
				if srv.URL != "https://localhost:8010/api/v1" {
					t.Errorf("URL = %q, want %q", srv.URL, "https://localhost:8010/api/v1")
				}
			},
		},
		{
			name: "corrupted JSON returns error",
			setup: func(t *testing.T, dir string) {
				writeConfigJSON(t, dir, "{invalid json!!!")
			},
			wantNil:     true,
			wantErr:     true,
			errContains: "corrupted",
		},
		{
			name: "nil servers map is initialized",
			setup: func(t *testing.T, dir string) {
				writeConfigJSON(t, dir, `{"default":"test","servers":null}`)
			},
			wantNil: false,
			wantErr: false,
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Servers == nil {
					t.Fatal("Servers should be initialized, got nil")
				}
			},
		},
		{
			name: "zero default_limit gets filled with default",
			setup: func(t *testing.T, dir string) {
				writeConfigJSON(t, dir, `{"default":"","settings":{"default_limit":0,"output_format":""},"servers":{}}`)
			},
			wantNil: false,
			wantErr: false,
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Settings.DefaultLimit != DefaultLimit {
					t.Errorf("DefaultLimit = %d, want %d", cfg.Settings.DefaultLimit, DefaultLimit)
				}
				if cfg.Settings.OutputFormat != DefaultOutput {
					t.Errorf("OutputFormat = %q, want %q", cfg.Settings.OutputFormat, DefaultOutput)
				}
			},
		},
		{
			name: "empty output_format gets filled with default",
			setup: func(t *testing.T, dir string) {
				writeConfigJSON(t, dir, `{"settings":{"default_limit":25},"servers":{}}`)
			},
			wantNil: false,
			wantErr: false,
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Settings.DefaultLimit != 25 {
					t.Errorf("DefaultLimit = %d, want 25", cfg.Settings.DefaultLimit)
				}
				if cfg.Settings.OutputFormat != DefaultOutput {
					t.Errorf("OutputFormat = %q, want %q", cfg.Settings.OutputFormat, DefaultOutput)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := setTestHome(t)
			tt.setup(t, dir)

			cfg, err := Load()

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" {
					if !containsStr(err.Error(), tt.errContains) {
						t.Errorf("error = %q, want it to contain %q", err.Error(), tt.errContains)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantNil {
				if cfg != nil {
					t.Errorf("expected nil config, got %+v", cfg)
				}
				return
			}
			if cfg == nil {
				t.Fatal("expected non-nil config, got nil")
			}
			if tt.validate != nil {
				tt.validate(t, cfg)
			}
		})
	}
}

func TestSave(t *testing.T) {
	t.Run("save and load roundtrip", func(t *testing.T) {
		setTestHome(t)

		original := &Config{
			Default: "prod",
			Settings: Settings{
				DefaultLimit: 75,
				OutputFormat: "json",
				ShowSystem:   true,
				TLSVerify:    true,
			},
			Servers: map[string]ServerConfig{
				"prod": {
					URL:       "https://prod:8010/api/v1",
					User:      "admin",
					Password:  EncodePassword("s3cret"),
					AuthMode:  "cam",
					Namespace: "LDAP",
				},
			},
		}

		if err := Save(original); err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		loaded, err := Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if loaded == nil {
			t.Fatal("loaded config is nil")
		}

		if loaded.Default != original.Default {
			t.Errorf("Default = %q, want %q", loaded.Default, original.Default)
		}
		if loaded.Settings.DefaultLimit != original.Settings.DefaultLimit {
			t.Errorf("DefaultLimit = %d, want %d", loaded.Settings.DefaultLimit, original.Settings.DefaultLimit)
		}
		if loaded.Settings.OutputFormat != original.Settings.OutputFormat {
			t.Errorf("OutputFormat = %q, want %q", loaded.Settings.OutputFormat, original.Settings.OutputFormat)
		}
		srv, ok := loaded.Servers["prod"]
		if !ok {
			t.Fatal("server 'prod' not found after load")
		}
		if srv.URL != "https://prod:8010/api/v1" {
			t.Errorf("URL = %q, want %q", srv.URL, "https://prod:8010/api/v1")
		}
		if srv.Namespace != "LDAP" {
			t.Errorf("Namespace = %q, want %q", srv.Namespace, "LDAP")
		}
	})

	t.Run("config file has 0600 permissions", func(t *testing.T) {
		setTestHome(t)

		cfg := NewConfig()
		if err := Save(cfg); err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		cfgPath, err := globalConfigPath()
		if err != nil {
			t.Fatalf("globalConfigPath failed: %v", err)
		}
		info, err := os.Stat(cfgPath)
		if err != nil {
			t.Fatalf("cannot stat config file: %v", err)
		}

		perm := info.Mode().Perm()
		if perm != 0600 {
			t.Errorf("permissions = %o, want 0600", perm)
		}
	})
}

func TestEncodeDecodePassword(t *testing.T) {
	tests := []struct {
		name     string
		password string
	}{
		{name: "simple password", password: "secret"},
		{name: "empty password", password: ""},
		{name: "password with special chars", password: "p@ss=w0rd!#$%&"},
		{name: "unicode password", password: "密碼テスト"},
		{name: "long password", password: "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodePassword(tt.password)
			decoded, err := DecodePassword(encoded)
			if err != nil {
				t.Fatalf("DecodePassword failed: %v", err)
			}
			if decoded != tt.password {
				t.Errorf("roundtrip failed: got %q, want %q", decoded, tt.password)
			}
		})
	}

	t.Run("invalid base64 returns error", func(t *testing.T) {
		_, err := DecodePassword("!!!not-valid-base64!!!")
		if err == nil {
			t.Fatal("expected error for invalid base64, got nil")
		}
		if !containsStr(err.Error(), "cannot decode password") {
			t.Errorf("error = %q, want it to contain 'cannot decode password'", err.Error())
		}
	})
}

func TestGetServer(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *Config
		serverName  string
		wantErr     bool
		errContains string
		wantURL     string
	}{
		{
			name: "found by name",
			cfg: &Config{
				Default: "dev",
				Servers: map[string]ServerConfig{
					"dev":  {URL: "https://dev:8010/api/v1"},
					"prod": {URL: "https://prod:8010/api/v1"},
				},
			},
			serverName: "prod",
			wantURL:    "https://prod:8010/api/v1",
		},
		{
			name: "not found returns error",
			cfg: &Config{
				Default: "dev",
				Servers: map[string]ServerConfig{
					"dev": {URL: "https://dev:8010/api/v1"},
				},
			},
			serverName:  "nonexistent",
			wantErr:     true,
			errContains: "not found",
		},
		{
			name: "empty name uses default",
			cfg: &Config{
				Default: "dev",
				Servers: map[string]ServerConfig{
					"dev": {URL: "https://dev:8010/api/v1"},
				},
			},
			serverName: "",
			wantURL:    "https://dev:8010/api/v1",
		},
		{
			name: "empty name with empty default returns error",
			cfg: &Config{
				Default: "",
				Servers: map[string]ServerConfig{},
			},
			serverName:  "",
			wantErr:     true,
			errContains: "No connection configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, err := tt.cfg.GetServer(tt.serverName)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !containsStr(err.Error(), tt.errContains) {
					t.Errorf("error = %q, want it to contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if srv.URL != tt.wantURL {
				t.Errorf("URL = %q, want %q", srv.URL, tt.wantURL)
			}
		})
	}
}

func TestGetEffectivePassword(t *testing.T) {
	t.Run("returns decoded password from config", func(t *testing.T) {
		cfg := &Config{
			Default: "dev",
			Servers: map[string]ServerConfig{
				"dev": {
					URL:      "https://dev:8010/api/v1",
					User:     "admin",
					Password: EncodePassword("configpass"),
					AuthMode: "basic",
				},
			},
		}

		pass, err := cfg.GetEffectivePassword("dev")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pass != "configpass" {
			t.Errorf("password = %q, want %q", pass, "configpass")
		}
	})

	t.Run("env var overrides config password", func(t *testing.T) {
		t.Setenv("TM1CLI_PASSWORD", "envpass")

		cfg := &Config{
			Default: "dev",
			Servers: map[string]ServerConfig{
				"dev": {
					URL:      "https://dev:8010/api/v1",
					User:     "admin",
					Password: EncodePassword("configpass"),
					AuthMode: "basic",
				},
			},
		}

		pass, err := cfg.GetEffectivePassword("dev")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pass != "envpass" {
			t.Errorf("password = %q, want %q", pass, "envpass")
		}
	})

	t.Run("returns error for unknown server when no env var", func(t *testing.T) {
		cfg := &Config{
			Default: "dev",
			Servers: map[string]ServerConfig{},
		}

		_, err := cfg.GetEffectivePassword("missing")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestAddServer(t *testing.T) {
	t.Run("first server becomes default", func(t *testing.T) {
		cfg := NewConfig()
		cfg.AddServer("first", ServerConfig{URL: "https://first:8010/api/v1"})

		if cfg.Default != "first" {
			t.Errorf("Default = %q, want %q", cfg.Default, "first")
		}
		if _, ok := cfg.Servers["first"]; !ok {
			t.Error("server 'first' not found in Servers map")
		}
	})

	t.Run("subsequent server does not override default", func(t *testing.T) {
		cfg := NewConfig()
		cfg.AddServer("first", ServerConfig{URL: "https://first:8010/api/v1"})
		cfg.AddServer("second", ServerConfig{URL: "https://second:8010/api/v1"})

		if cfg.Default != "first" {
			t.Errorf("Default = %q, want %q", cfg.Default, "first")
		}
		if len(cfg.Servers) != 2 {
			t.Errorf("Servers count = %d, want 2", len(cfg.Servers))
		}
	})

	t.Run("adding to empty default sets default", func(t *testing.T) {
		cfg := &Config{
			Default:  "",
			Settings: DefaultSettings(),
			Servers:  map[string]ServerConfig{"existing": {URL: "https://existing:8010/api/v1"}},
		}
		// Default is empty but there's already a server — AddServer with a new server
		// The condition is: Default == "" OR len(Servers) == 1
		cfg.AddServer("new", ServerConfig{URL: "https://new:8010/api/v1"})

		if cfg.Default != "new" {
			t.Errorf("Default = %q, want %q", cfg.Default, "new")
		}
	})
}

func TestRemoveServer(t *testing.T) {
	t.Run("removes server and updates default", func(t *testing.T) {
		cfg := NewConfig()
		cfg.AddServer("first", ServerConfig{URL: "https://first:8010/api/v1"})
		cfg.AddServer("second", ServerConfig{URL: "https://second:8010/api/v1"})
		// Default is "first"

		newDefault := cfg.RemoveServer("first")

		if _, ok := cfg.Servers["first"]; ok {
			t.Error("server 'first' should have been removed")
		}
		if newDefault != "second" {
			t.Errorf("newDefault = %q, want %q", newDefault, "second")
		}
		if cfg.Default != "second" {
			t.Errorf("Default = %q, want %q", cfg.Default, "second")
		}
	})

	t.Run("removes non-default server without changing default", func(t *testing.T) {
		cfg := NewConfig()
		cfg.AddServer("first", ServerConfig{URL: "https://first:8010/api/v1"})
		cfg.AddServer("second", ServerConfig{URL: "https://second:8010/api/v1"})

		newDefault := cfg.RemoveServer("second")

		if newDefault != "first" {
			t.Errorf("newDefault = %q, want %q", newDefault, "first")
		}
		if cfg.Default != "first" {
			t.Errorf("Default = %q, want %q", cfg.Default, "first")
		}
	})

	t.Run("removing last server clears default", func(t *testing.T) {
		cfg := NewConfig()
		cfg.AddServer("only", ServerConfig{URL: "https://only:8010/api/v1"})

		newDefault := cfg.RemoveServer("only")

		if newDefault != "" {
			t.Errorf("newDefault = %q, want empty string", newDefault)
		}
		if cfg.Default != "" {
			t.Errorf("Default = %q, want empty string", cfg.Default)
		}
		if len(cfg.Servers) != 0 {
			t.Errorf("Servers count = %d, want 0", len(cfg.Servers))
		}
	})
}

func TestGetEffectiveOutput(t *testing.T) {
	t.Run("returns config output format when no env var", func(t *testing.T) {
		cfg := &Config{Settings: Settings{OutputFormat: "json"}}
		result := cfg.GetEffectiveOutput()
		if result != "json" {
			t.Errorf("got %q, want %q", result, "json")
		}
	})

	t.Run("env var overrides config output format", func(t *testing.T) {
		t.Setenv("TM1CLI_OUTPUT", "json")
		cfg := &Config{Settings: Settings{OutputFormat: "table"}}
		result := cfg.GetEffectiveOutput()
		if result != "json" {
			t.Errorf("got %q, want %q", result, "json")
		}
	})

	t.Run("returns default when config is empty and no env var", func(t *testing.T) {
		cfg := &Config{Settings: Settings{OutputFormat: ""}}
		result := cfg.GetEffectiveOutput()
		if result != DefaultOutput {
			t.Errorf("got %q, want %q", result, DefaultOutput)
		}
	})
}

func TestGetEffectiveServer(t *testing.T) {
	t.Run("returns config default when no env var", func(t *testing.T) {
		cfg := &Config{Default: "myserver"}

		result := cfg.GetEffectiveServer()
		if result != "myserver" {
			t.Errorf("got %q, want %q", result, "myserver")
		}
	})

	t.Run("env var overrides config default", func(t *testing.T) {
		t.Setenv("TM1CLI_SERVER", "envserver")

		cfg := &Config{Default: "myserver"}

		result := cfg.GetEffectiveServer()
		if result != "envserver" {
			t.Errorf("got %q, want %q", result, "envserver")
		}
	})

	t.Run("returns empty string when no default and no env var", func(t *testing.T) {
		cfg := &Config{Default: ""}

		result := cfg.GetEffectiveServer()
		if result != "" {
			t.Errorf("got %q, want empty string", result)
		}
	})
}

func TestFindLocalConfig(t *testing.T) {
	t.Run("finds config in current directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		t.Setenv("HOME", filepath.Join(t.TempDir(), "fakehome")) // avoid global path collision
		t.Setenv("TM1CLI_CONFIG", "")
		t.Chdir(tmpDir)

		cfgDir := filepath.Join(tmpDir, ".tm1cli")
		os.MkdirAll(cfgDir, 0700)
		os.WriteFile(filepath.Join(cfgDir, "config.json"), []byte(`{}`), 0600)

		result := findLocalConfig(globalPathForTest(t))
		expected := filepath.Join(tmpDir, ".tm1cli", "config.json")
		if result != expected {
			t.Errorf("findLocalConfig(globalPathForTest(t)) = %q, want %q", result, expected)
		}
	})

	t.Run("walks parent directories", func(t *testing.T) {
		tmpDir := t.TempDir()
		t.Setenv("HOME", filepath.Join(t.TempDir(), "fakehome"))
		t.Setenv("TM1CLI_CONFIG", "")

		cfgDir := filepath.Join(tmpDir, ".tm1cli")
		os.MkdirAll(cfgDir, 0700)
		os.WriteFile(filepath.Join(cfgDir, "config.json"), []byte(`{}`), 0600)

		childDir := filepath.Join(tmpDir, "sub", "deep")
		os.MkdirAll(childDir, 0755)
		t.Chdir(childDir)

		result := findLocalConfig(globalPathForTest(t))
		expected := filepath.Join(tmpDir, ".tm1cli", "config.json")
		if result != expected {
			t.Errorf("findLocalConfig(globalPathForTest(t)) = %q, want %q", result, expected)
		}
	})

	t.Run("returns empty when no local config", func(t *testing.T) {
		tmpDir := t.TempDir()
		t.Setenv("HOME", filepath.Join(t.TempDir(), "fakehome"))
		t.Setenv("TM1CLI_CONFIG", "")
		t.Chdir(tmpDir)

		result := findLocalConfig(globalPathForTest(t))
		if result != "" {
			t.Errorf("findLocalConfig(globalPathForTest(t)) = %q, want empty string", result)
		}
	})

	t.Run("does not return global config as local", func(t *testing.T) {
		tmpHome := t.TempDir()
		t.Setenv("HOME", tmpHome)
		t.Setenv("TM1CLI_CONFIG", "")

		// Create global config
		globalDir := filepath.Join(tmpHome, ".tm1cli")
		os.MkdirAll(globalDir, 0700)
		os.WriteFile(filepath.Join(globalDir, "config.json"), []byte(`{}`), 0600)

		// cd into home — global config exists but should NOT be found as "local"
		t.Chdir(tmpHome)

		result := findLocalConfig(globalPathForTest(t))
		if result != "" {
			t.Errorf("findLocalConfig(globalPathForTest(t)) should not return global config, got %q", result)
		}
	})
}

func TestLoadPrecedence(t *testing.T) {
	t.Run("TM1CLI_CONFIG env var overrides all", func(t *testing.T) {
		tmpHome := t.TempDir()
		t.Setenv("HOME", tmpHome)
		t.Chdir(tmpHome)

		// Create global config
		globalDir := filepath.Join(tmpHome, ".tm1cli")
		os.MkdirAll(globalDir, 0700)
		writeConfigJSON(t, globalDir, `{"default":"global-srv","servers":{"global-srv":{"url":"https://global:8010/api/v1","user":"admin","password":"","auth_mode":"basic"}},"settings":{"default_limit":50,"output_format":"table"}}`)

		// Create env config at custom path
		envDir := filepath.Join(t.TempDir(), "custom")
		os.MkdirAll(envDir, 0700)
		envPath := filepath.Join(envDir, "config.json")
		os.WriteFile(envPath, []byte(`{"default":"env-srv","servers":{"env-srv":{"url":"https://env:8010/api/v1","user":"admin","password":"","auth_mode":"basic"}},"settings":{"default_limit":50,"output_format":"table"}}`), 0600)
		t.Setenv("TM1CLI_CONFIG", envPath)

		cfg, err := Load()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg == nil {
			t.Fatal("expected config, got nil")
		}
		if cfg.Default != "env-srv" {
			t.Errorf("Default = %q, want %q", cfg.Default, "env-srv")
		}
		if cfg.ConfigSource() != SourceEnv {
			t.Errorf("ConfigSource() = %q, want %q", cfg.ConfigSource(), SourceEnv)
		}
	})

	t.Run("local config preferred over global", func(t *testing.T) {
		tmpHome := t.TempDir()
		t.Setenv("HOME", tmpHome)
		t.Setenv("TM1CLI_CONFIG", "")

		// Create global config
		globalDir := filepath.Join(tmpHome, ".tm1cli")
		os.MkdirAll(globalDir, 0700)
		writeConfigJSON(t, globalDir, `{"default":"global-srv","servers":{"global-srv":{"url":"https://global:8010/api/v1","user":"admin","password":"","auth_mode":"basic"}},"settings":{"default_limit":50,"output_format":"table"}}`)

		// Create local config in a project dir
		projectDir := filepath.Join(t.TempDir(), "myproject")
		os.MkdirAll(projectDir, 0755)
		localDir := filepath.Join(projectDir, ".tm1cli")
		os.MkdirAll(localDir, 0700)
		os.WriteFile(filepath.Join(localDir, "config.json"), []byte(`{"default":"local-srv","servers":{"local-srv":{"url":"https://local:8010/api/v1","user":"admin","password":"","auth_mode":"basic"}},"settings":{"default_limit":50,"output_format":"table"}}`), 0600)

		t.Chdir(projectDir)

		cfg, err := Load()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg == nil {
			t.Fatal("expected config, got nil")
		}
		if cfg.Default != "local-srv" {
			t.Errorf("Default = %q, want %q", cfg.Default, "local-srv")
		}
		if cfg.ConfigSource() != SourceLocal {
			t.Errorf("ConfigSource() = %q, want %q", cfg.ConfigSource(), SourceLocal)
		}
	})

	t.Run("falls back to global when no local config", func(t *testing.T) {
		tmpHome := t.TempDir()
		t.Setenv("HOME", tmpHome)
		t.Setenv("TM1CLI_CONFIG", "")

		// Create global config
		globalDir := filepath.Join(tmpHome, ".tm1cli")
		os.MkdirAll(globalDir, 0700)
		writeConfigJSON(t, globalDir, `{"default":"global-srv","servers":{"global-srv":{"url":"https://global:8010/api/v1","user":"admin","password":"","auth_mode":"basic"}},"settings":{"default_limit":50,"output_format":"table"}}`)

		// cd into empty dir (no local config)
		emptyDir := t.TempDir()
		t.Chdir(emptyDir)

		cfg, err := Load()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg == nil {
			t.Fatal("expected config, got nil")
		}
		if cfg.Default != "global-srv" {
			t.Errorf("Default = %q, want %q", cfg.Default, "global-srv")
		}
		if cfg.ConfigSource() != SourceGlobal {
			t.Errorf("ConfigSource() = %q, want %q", cfg.ConfigSource(), SourceGlobal)
		}
	})
}

func TestSavePrecedence(t *testing.T) {
	t.Run("writes to loadedFrom path", func(t *testing.T) {
		tmpDir := t.TempDir()
		t.Setenv("HOME", filepath.Join(t.TempDir(), "fakehome"))
		t.Setenv("TM1CLI_CONFIG", "")
		t.Chdir(tmpDir)

		// Create local config
		localDir := filepath.Join(tmpDir, ".tm1cli")
		os.MkdirAll(localDir, 0700)
		os.WriteFile(filepath.Join(localDir, "config.json"), []byte(`{"default":"old","servers":{"old":{"url":"https://old:8010/api/v1","user":"admin","password":"","auth_mode":"basic"}},"settings":{"default_limit":50,"output_format":"table"}}`), 0600)

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		cfg.Default = "updated"
		if err := Save(cfg); err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		// Verify saved to local path
		data, err := os.ReadFile(filepath.Join(localDir, "config.json"))
		if err != nil {
			t.Fatalf("cannot read saved config: %v", err)
		}
		var saved Config
		json.Unmarshal(data, &saved)
		if saved.Default != "updated" {
			t.Errorf("Default = %q, want %q", saved.Default, "updated")
		}
	})

	t.Run("new config resolves path via TM1CLI_CONFIG", func(t *testing.T) {
		tmpDir := t.TempDir()
		t.Setenv("HOME", filepath.Join(t.TempDir(), "fakehome"))
		t.Chdir(tmpDir)

		envPath := filepath.Join(tmpDir, "custom", "config.json")
		t.Setenv("TM1CLI_CONFIG", envPath)

		cfg := NewConfig()
		cfg.AddServer("test", ServerConfig{URL: "https://test:8010/api/v1", User: "admin", AuthMode: "basic"})

		if err := Save(cfg); err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		// Verify file was created at env path
		if _, err := os.Stat(envPath); os.IsNotExist(err) {
			t.Fatalf("config was not saved to TM1CLI_CONFIG path %q", envPath)
		}

		// Verify loadedFrom was set
		if cfg.LoadedFrom() != envPath {
			t.Errorf("LoadedFrom() = %q, want %q", cfg.LoadedFrom(), envPath)
		}
	})

	t.Run("creates parent directories", func(t *testing.T) {
		tmpDir := t.TempDir()
		t.Setenv("HOME", filepath.Join(t.TempDir(), "fakehome"))
		t.Chdir(tmpDir)

		envPath := filepath.Join(tmpDir, "deep", "nested", ".tm1cli", "config.json")
		t.Setenv("TM1CLI_CONFIG", envPath)

		cfg := NewConfig()
		if err := Save(cfg); err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		if _, err := os.Stat(envPath); os.IsNotExist(err) {
			t.Fatal("config file should have been created with parent dirs")
		}
	})
}

func TestConfigSource(t *testing.T) {
	t.Run("returns env when TM1CLI_CONFIG is set", func(t *testing.T) {
		tmpDir := t.TempDir()
		t.Setenv("HOME", filepath.Join(t.TempDir(), "fakehome"))
		t.Chdir(tmpDir)

		envPath := filepath.Join(tmpDir, "env-config.json")
		os.WriteFile(envPath, []byte(`{"default":"","servers":{},"settings":{"default_limit":50,"output_format":"table"}}`), 0600)
		t.Setenv("TM1CLI_CONFIG", envPath)

		cfg, err := Load()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ConfigSource() != SourceEnv {
			t.Errorf("ConfigSource() = %q, want %q", cfg.ConfigSource(), SourceEnv)
		}
	})

	t.Run("returns local for project config", func(t *testing.T) {
		tmpDir := t.TempDir()
		t.Setenv("HOME", filepath.Join(t.TempDir(), "fakehome"))
		t.Setenv("TM1CLI_CONFIG", "")
		t.Chdir(tmpDir)

		localDir := filepath.Join(tmpDir, ".tm1cli")
		os.MkdirAll(localDir, 0700)
		os.WriteFile(filepath.Join(localDir, "config.json"), []byte(`{"default":"","servers":{},"settings":{"default_limit":50,"output_format":"table"}}`), 0600)

		cfg, err := Load()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ConfigSource() != SourceLocal {
			t.Errorf("ConfigSource() = %q, want %q", cfg.ConfigSource(), SourceLocal)
		}
	})

	t.Run("returns global when only global exists", func(t *testing.T) {
		dir := setTestHome(t)
		writeConfigJSON(t, dir, `{"default":"","servers":{},"settings":{"default_limit":50,"output_format":"table"}}`)

		cfg, err := Load()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ConfigSource() != SourceGlobal {
			t.Errorf("ConfigSource() = %q, want %q", cfg.ConfigSource(), SourceGlobal)
		}
	})
}

// --- keychain storage tests ---

func TestStorePassword_UsesKeychainWhenAvailable(t *testing.T) {
	keyring.MockInit()
	srv := &ServerConfig{}
	used, warning := StorePassword(srv, "hunter2")
	if !used || warning != "" {
		t.Fatalf("used=%v warning=%q, want used=true warning=\"\"", used, warning)
	}
	if srv.Password != "" {
		t.Errorf("Password = %q, want empty", srv.Password)
	}
	if srv.PasswordStorage != PasswordStorageKeychain {
		t.Errorf("PasswordStorage = %q, want %q", srv.PasswordStorage, PasswordStorageKeychain)
	}
	if srv.PasswordRef == "" {
		t.Error("PasswordRef should be populated")
	}
	got, _ := GetKeychainPassword(srv.PasswordRef)
	if got != "hunter2" {
		t.Errorf("keychain value = %q, want %q", got, "hunter2")
	}
}

func TestStorePassword_FallsBackToBase64OnKeychainFailure(t *testing.T) {
	t.Cleanup(OverrideKeychainSet(func(service, user, password string) error {
		return errors.New("simulated keychain failure")
	}))
	srv := &ServerConfig{}
	used, warning := StorePassword(srv, "hunter2")
	if used {
		t.Errorf("used = true, want false")
	}
	if warning == "" {
		t.Error("warning should not be empty on fallback")
	}
	if srv.PasswordStorage != PasswordStorageBase64 {
		t.Errorf("PasswordStorage = %q, want %q", srv.PasswordStorage, PasswordStorageBase64)
	}
	decoded, err := DecodePassword(srv.Password)
	if err != nil || decoded != "hunter2" {
		t.Errorf("DecodePassword = %q, %v; want hunter2, nil", decoded, err)
	}
	if srv.PasswordRef != "" {
		t.Errorf("PasswordRef = %q, want empty on fallback", srv.PasswordRef)
	}
}

func TestStorePassword_FallbackCleansUpOldKeychainEntry(t *testing.T) {
	keyring.MockInit()
	// Seed an existing keychain-backed server.
	oldRef := "orphan-candidate"
	if err := SetKeychainPassword(oldRef, "oldpw"); err != nil {
		t.Fatalf("setup: %v", err)
	}
	srv := &ServerConfig{PasswordStorage: PasswordStorageKeychain, PasswordRef: oldRef}

	// Inject a keychain failure so StorePassword falls back to base64.
	t.Cleanup(OverrideKeychainSet(func(service, user, password string) error {
		return errors.New("simulated keychain failure")
	}))

	used, _ := StorePassword(srv, "newpw")
	if used {
		t.Fatalf("used = true, want false (fallback expected)")
	}

	// Restore the real Set so we can verify the old entry is gone.
	// OverrideKeychainSet's cleanup runs after this test, but we need
	// the real Get semantics now. Since the mock keeps its own map and
	// only Set was overridden, Get still works against the mock map.
	if _, err := GetKeychainPassword(oldRef); !errors.Is(err, ErrKeychainNotFound) {
		t.Errorf("old keychain entry should have been deleted after fallback, err = %v", err)
	}
}

func TestGetEffectivePassword_KeychainStorage(t *testing.T) {
	keyring.MockInit()
	ref := "test-ref-123"
	_ = SetKeychainPassword(ref, "kc-password")
	cfg := &Config{
		Default: "dev",
		Servers: map[string]ServerConfig{
			"dev": {
				PasswordStorage: PasswordStorageKeychain,
				PasswordRef:     ref,
			},
		},
	}
	got, err := cfg.GetEffectivePassword("dev")
	if err != nil || got != "kc-password" {
		t.Errorf("got %q, %v; want kc-password, nil", got, err)
	}
}

func TestGetEffectivePassword_KeychainMissingReturnsError(t *testing.T) {
	keyring.MockInit()
	cfg := &Config{
		Default: "dev",
		Servers: map[string]ServerConfig{
			"dev": {
				PasswordStorage: PasswordStorageKeychain,
				PasswordRef:     "nonexistent",
			},
		},
	}
	_, err := cfg.GetEffectivePassword("dev")
	if err == nil {
		t.Fatal("expected error for missing keychain entry, got nil")
	}
	if !containsStr(err.Error(), "config edit") {
		t.Errorf("error = %q, want it to mention 'config edit' for recovery", err.Error())
	}
}

func TestGetEffectivePassword_LegacyBase64StillWorks(t *testing.T) {
	cfg := &Config{
		Default: "dev",
		Servers: map[string]ServerConfig{
			"dev": {
				Password: EncodePassword("legacypw"),
				// PasswordStorage left empty (legacy)
			},
		},
	}
	got, err := cfg.GetEffectivePassword("dev")
	if err != nil || got != "legacypw" {
		t.Errorf("got %q, %v; want legacypw, nil", got, err)
	}
}

func TestGetEffectivePassword_EnvVarOverridesKeychain(t *testing.T) {
	t.Setenv("TM1CLI_PASSWORD", "env-wins")
	keyring.MockInit()
	cfg := &Config{
		Default: "dev",
		Servers: map[string]ServerConfig{
			"dev": {
				PasswordStorage: PasswordStorageKeychain,
				PasswordRef:     "some-ref",
				// no keychain entry set — env var must short-circuit
			},
		},
	}
	got, err := cfg.GetEffectivePassword("dev")
	if err != nil || got != "env-wins" {
		t.Errorf("got %q, %v; want env-wins, nil", got, err)
	}
}

func TestClearStoredPassword_RemovesFromKeychain(t *testing.T) {
	keyring.MockInit()
	ref := "clear-ref"
	_ = SetKeychainPassword(ref, "x")
	srv := &ServerConfig{PasswordStorage: PasswordStorageKeychain, PasswordRef: ref}
	if err := ClearStoredPassword(srv); err != nil {
		t.Fatalf("ClearStoredPassword failed: %v", err)
	}
	if _, err := GetKeychainPassword(ref); !errors.Is(err, ErrKeychainNotFound) {
		t.Errorf("entry should be gone, err = %v", err)
	}
}

func TestClearStoredPassword_Base64IsNoOp(t *testing.T) {
	srv := &ServerConfig{PasswordStorage: PasswordStorageBase64, Password: "xyz"}
	if err := ClearStoredPassword(srv); err != nil {
		t.Errorf("Base64 ClearStoredPassword should be no-op, got %v", err)
	}
	if srv.Password != "xyz" {
		t.Errorf("Base64 password should not be modified, got %q", srv.Password)
	}
}

func TestNewPasswordRef_UniqueAndValid(t *testing.T) {
	a := newPasswordRef()
	b := newPasswordRef()
	if a == b {
		t.Error("newPasswordRef should return unique values")
	}
	if len(a) != 32 {
		t.Errorf("len = %d, want 32 hex chars", len(a))
	}
}

// containsStr is a simple helper to check string containment.
func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && searchStr(s, substr)
}

func searchStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
