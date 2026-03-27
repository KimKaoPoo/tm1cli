package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// setTestHome overrides HOME so that configDir() and ConfigPath() point to a temp directory.
// Returns the path to the config dir (~/.tm1cli) within that temp home.
func setTestHome(t *testing.T) string {
	t.Helper()
	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)
	dir := filepath.Join(tmpHome, ".tm1cli")
	return dir
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

		cfgPath, err := ConfigPath()
		if err != nil {
			t.Fatalf("ConfigPath failed: %v", err)
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
