package cmd

import (
	"testing"
	"tm1cli/internal/config"
)

func TestGetOutputFormat(t *testing.T) {
	tests := []struct {
		name       string
		flagValue  string
		envValue   string
		cfgFormat  string
		cfgNil     bool
		wantResult string
	}{
		{
			name:       "flag takes priority over everything",
			flagValue:  "json",
			envValue:   "table",
			cfgFormat:  "table",
			wantResult: "json",
		},
		{
			name:       "env var takes priority over config",
			flagValue:  "",
			envValue:   "json",
			cfgFormat:  "table",
			wantResult: "json",
		},
		{
			name:       "config value used when no flag or env",
			flagValue:  "",
			envValue:   "",
			cfgFormat:  "json",
			wantResult: "json",
		},
		{
			name:       "default when no flag, no env, no config",
			flagValue:  "",
			envValue:   "",
			cfgNil:     true,
			wantResult: config.DefaultOutput,
		},
		{
			name:       "default when config is nil and no flag or env",
			flagValue:  "",
			envValue:   "",
			cfgNil:     true,
			wantResult: "table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save and restore the global flag variable
			origFlag := flagOutput
			defer func() { flagOutput = origFlag }()
			flagOutput = tt.flagValue

			if tt.envValue != "" {
				t.Setenv("TM1CLI_OUTPUT", tt.envValue)
			}

			var cfg *config.Config
			if !tt.cfgNil {
				cfg = &config.Config{
					Settings: config.Settings{
						OutputFormat: tt.cfgFormat,
					},
				}
			}

			result := getOutputFormat(cfg)
			if result != tt.wantResult {
				t.Errorf("getOutputFormat() = %q, want %q", result, tt.wantResult)
			}
		})
	}
}

func TestGetLimit(t *testing.T) {
	tests := []struct {
		name      string
		flagLimit int
		flagAll   bool
		cfgLimit  int
		cfgNil    bool
		want      int
	}{
		{
			name:    "flagAll returns 0 (no limit)",
			flagAll: true,
			want:    0,
		},
		{
			name:      "flagLimit takes priority",
			flagLimit: 25,
			cfgLimit:  100,
			want:      25,
		},
		{
			name:     "config limit used when no flags",
			cfgLimit: 75,
			want:     75,
		},
		{
			name:   "default when config is nil",
			cfgNil: true,
			want:   config.DefaultLimit,
		},
		{
			name:     "default when config limit is 0",
			cfgLimit: 0,
			want:     config.DefaultLimit,
		},
		{
			name:      "flagAll overrides flagLimit",
			flagAll:   true,
			flagLimit: 25,
			want:      0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg *config.Config
			if !tt.cfgNil {
				cfg = &config.Config{
					Settings: config.Settings{
						DefaultLimit: tt.cfgLimit,
					},
				}
			}

			result := getLimit(cfg, tt.flagLimit, tt.flagAll)
			if result != tt.want {
				t.Errorf("getLimit() = %d, want %d", result, tt.want)
			}
		})
	}
}

func TestGetShowSystem(t *testing.T) {
	tests := []struct {
		name           string
		flagShowSystem bool
		cfgShowSystem  bool
		cfgNil         bool
		want           bool
	}{
		{
			name:           "flag true overrides config false",
			flagShowSystem: true,
			cfgShowSystem:  false,
			want:           true,
		},
		{
			name:           "flag false uses config true",
			flagShowSystem: false,
			cfgShowSystem:  true,
			want:           true,
		},
		{
			name:           "flag false uses config false",
			flagShowSystem: false,
			cfgShowSystem:  false,
			want:           false,
		},
		{
			name:           "flag true with nil config",
			flagShowSystem: true,
			cfgNil:         true,
			want:           true,
		},
		{
			name:           "default when config is nil and flag is false",
			flagShowSystem: false,
			cfgNil:         true,
			want:           config.DefaultShowSystem,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg *config.Config
			if !tt.cfgNil {
				cfg = &config.Config{
					Settings: config.Settings{
						ShowSystem: tt.cfgShowSystem,
					},
				}
			}

			result := getShowSystem(cfg, tt.flagShowSystem)
			if result != tt.want {
				t.Errorf("getShowSystem() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestIsJSONOutput(t *testing.T) {
	tests := []struct {
		name       string
		flagValue  string
		cfgFormat  string
		cfgNil     bool
		wantResult bool
	}{
		{
			name:       "true when flag is json",
			flagValue:  "json",
			wantResult: true,
		},
		{
			name:       "false when flag is table",
			flagValue:  "table",
			wantResult: false,
		},
		{
			name:       "true when config is json and no flag",
			flagValue:  "",
			cfgFormat:  "json",
			wantResult: true,
		},
		{
			name:       "false when config is table and no flag",
			flagValue:  "",
			cfgFormat:  "table",
			wantResult: false,
		},
		{
			name:       "false when config is nil (uses default table)",
			flagValue:  "",
			cfgNil:     true,
			wantResult: false,
		},
		{
			name:       "false for unknown format",
			flagValue:  "csv",
			wantResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origFlag := flagOutput
			defer func() { flagOutput = origFlag }()
			flagOutput = tt.flagValue

			var cfg *config.Config
			if !tt.cfgNil {
				cfg = &config.Config{
					Settings: config.Settings{
						OutputFormat: tt.cfgFormat,
					},
				}
			}

			result := isJSONOutput(cfg)
			if result != tt.wantResult {
				t.Errorf("isJSONOutput() = %v, want %v", result, tt.wantResult)
			}
		})
	}
}
