package cmd

import (
	"strings"
	"testing"
	"time"
)

func TestParseWatchInterval(t *testing.T) {
	tests := []struct {
		name         string
		intervalFlag string
		secondsFlag  int
		want         time.Duration
		wantErr      bool
		errContains  string
	}{
		{
			name:         "default 5s",
			intervalFlag: "5s",
			secondsFlag:  0,
			want:         5 * time.Second,
		},
		{
			name:         "10 seconds",
			intervalFlag: "10s",
			secondsFlag:  0,
			want:         10 * time.Second,
		},
		{
			name:         "1 minute",
			intervalFlag: "1m",
			secondsFlag:  0,
			want:         1 * time.Minute,
		},
		{
			name:         "minimum 1s",
			intervalFlag: "1s",
			secondsFlag:  0,
			want:         1 * time.Second,
		},
		{
			name:         "seconds flag overrides interval",
			intervalFlag: "5s",
			secondsFlag:  10,
			want:         10 * time.Second,
		},
		{
			name:         "seconds flag overrides invalid interval",
			intervalFlag: "abc",
			secondsFlag:  5,
			want:         5 * time.Second,
		},
		{
			name:         "seconds flag 1",
			intervalFlag: "30s",
			secondsFlag:  1,
			want:         1 * time.Second,
		},
		{
			name:         "500ms too short",
			intervalFlag: "500ms",
			secondsFlag:  0,
			wantErr:      true,
			errContains:  "at least 1s",
		},
		{
			name:         "0s too short",
			intervalFlag: "0s",
			secondsFlag:  0,
			wantErr:      true,
			errContains:  "at least 1s",
		},
		{
			name:         "invalid format",
			intervalFlag: "abc",
			secondsFlag:  0,
			wantErr:      true,
			errContains:  "Invalid interval",
		},
		{
			name:         "negative seconds ignored uses interval",
			intervalFlag: "5s",
			secondsFlag:  -1,
			want:         5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseWatchInterval(tt.intervalFlag, tt.secondsFlag)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errContains)
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error = %q, want containing %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWatchCommand_MissingDash(t *testing.T) {
	resetCmdFlags(t)

	out := captureStderr(t, func() {
		rootCmd.SetArgs([]string{"watch", "cubes"})
		rootCmd.Execute()
	})

	if !strings.Contains(out, "Missing '--' separator") {
		t.Errorf("expected missing separator error, got: %s", out)
	}
}

func TestWatchCommand_EmptyAfterDash(t *testing.T) {
	resetCmdFlags(t)

	out := captureStderr(t, func() {
		rootCmd.SetArgs([]string{"watch", "--"})
		rootCmd.Execute()
	})

	if !strings.Contains(out, "Missing command after '--'") {
		t.Errorf("expected missing command error, got: %s", out)
	}
}

func TestWatchCommand_FlagRegistration(t *testing.T) {
	if f := watchCmd.Flags().Lookup("interval"); f == nil {
		t.Error("--interval flag not registered")
	} else if f.DefValue != "5s" {
		t.Errorf("--interval default = %q, want %q", f.DefValue, "5s")
	}

	if f := watchCmd.Flags().ShorthandLookup("n"); f == nil {
		t.Error("-n flag not registered")
	} else if f.DefValue != "0" {
		t.Errorf("-n default = %q, want %q", f.DefValue, "0")
	}
}

func TestWatchCommand_RejectsJSONOutput(t *testing.T) {
	resetCmdFlags(t)

	out := captureStderr(t, func() {
		rootCmd.SetArgs([]string{"watch", "--output", "json", "--", "cubes"})
		rootCmd.Execute()
	})

	if !strings.Contains(out, "terminal-only") {
		t.Errorf("expected terminal-only rejection, got: %s", out)
	}
}
