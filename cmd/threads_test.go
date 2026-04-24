package cmd

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
	"tm1cli/internal/model"
)

// --- Unit: filterThreads ---

func TestFilterThreads(t *testing.T) {
	threads := []model.Thread{
		{ID: 1, Name: "Admin", State: "Run", ElapsedTime: 5.0},
		{ID: 2, Name: "UserA", State: "Idle", ElapsedTime: 20.0},
		{ID: 3, Name: "worker", State: "Run", ElapsedTime: 60.0},
	}

	tests := []struct {
		name       string
		user       string
		state      string
		minElapsed time.Duration
		wantIDs    []int64
	}{
		{
			name:    "no filters returns all",
			wantIDs: []int64{1, 2, 3},
		},
		{
			name:    "filter by user case-insensitively",
			user:    "admin",
			wantIDs: []int64{1},
		},
		{
			name:    "filter by state case-insensitively",
			state:   "run",
			wantIDs: []int64{1, 3},
		},
		{
			name:       "filter by min-elapsed",
			minElapsed: 10 * time.Second,
			wantIDs:    []int64{2, 3},
		},
		{
			name:    "all filtered out returns nil",
			state:   "CommitWait",
			wantIDs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterThreads(threads, tt.user, tt.state, tt.minElapsed)
			if len(got) != len(tt.wantIDs) {
				t.Fatalf("got %d threads, want %d", len(got), len(tt.wantIDs))
			}
			for i, id := range tt.wantIDs {
				if got[i].ID != id {
					t.Errorf("got[%d].ID = %d, want %d", i, got[i].ID, id)
				}
			}
		})
	}
}

// --- Unit: formatThreadDuration ---

func TestFormatThreadDuration(t *testing.T) {
	tests := []struct {
		secs float64
		want string
	}{
		{0.0, "0ms"},
		{0.5, "500ms"},
		{1.5, "1.5s"},
		{59.9, "59.9s"},
		{90.0, "1m30s"},
	}
	for _, tt := range tests {
		got := formatThreadDuration(model.ThreadDuration(tt.secs))
		if got != tt.want {
			t.Errorf("formatThreadDuration(%v) = %q, want %q", tt.secs, got, tt.want)
		}
	}
}

// --- Unit: parseODataDuration ---

func TestParseODataDuration(t *testing.T) {
	tests := []struct {
		input string
		want  float64
	}{
		{"PT10S", 10.0},
		{"PT1M30S", 90.0},
		{"PT1H0M0S", 3600.0},
		{"duration'PT10S'", 10.0},
		{"PT0.5S", 0.5},
	}
	for _, tt := range tests {
		got := parseODataDuration(tt.input)
		if got != tt.want {
			t.Errorf("parseODataDuration(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

// --- Unit: ThreadDuration JSON unmarshal ---

func TestThreadDurationUnmarshalJSON(t *testing.T) {
	t.Run("float64 value", func(t *testing.T) {
		var d model.ThreadDuration
		if err := json.Unmarshal([]byte("5.0"), &d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if float64(d) != 5.0 {
			t.Errorf("got %v, want 5.0", d)
		}
	})
	t.Run("ISO duration string", func(t *testing.T) {
		var d model.ThreadDuration
		if err := json.Unmarshal([]byte(`"PT10S"`), &d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if float64(d) != 10.0 {
			t.Errorf("got %v, want 10.0", d)
		}
	})
}

// --- Integration: threads list ---

func TestThreadsList(t *testing.T) {
	thread1 := model.Thread{ID: 1, Type: "User", Name: "Admin", Context: "REST", State: "Run", Function: "GET", ElapsedTime: 5.0, WaitTime: 0.1}
	thread2 := model.Thread{ID: 2, Type: "Worker", Name: "UserA", Context: "Chore", State: "Idle", Function: "None", ElapsedTime: 20.0, WaitTime: 0.0}

	tests := []struct {
		name       string
		args       []string
		threads    []model.Thread
		wantOut    []string
		wantErrOut []string
		wantExit   bool
	}{
		{
			name:    "table output lists threads",
			args:    []string{"threads", "list"},
			threads: []model.Thread{thread1, thread2},
			wantOut: []string{"Admin", "Run", "UserA", "Idle"},
		},
		{
			name:    "empty thread list shows zero summary",
			args:    []string{"threads", "list"},
			threads: []model.Thread{},
			wantOut: []string{"Showing 0 of 0"},
		},
		{
			name:    "filter by state",
			args:    []string{"threads", "list", "--state", "Run"},
			threads: []model.Thread{thread1, thread2},
			wantOut: []string{"Admin"},
		},
		{
			name:    "filter by user",
			args:    []string{"threads", "list", "--user", "admin"},
			threads: []model.Thread{thread1, thread2},
			wantOut: []string{"Admin"},
		},
		{
			name:    "filter by min-elapsed",
			args:    []string{"threads", "list", "--min-elapsed", "10s"},
			threads: []model.Thread{thread1, thread2},
			wantOut: []string{"UserA"},
		},
		{
			name:     "invalid min-elapsed returns error",
			args:     []string{"threads", "list", "--min-elapsed", "bad"},
			threads:  []model.Thread{thread1},
			wantExit: true,
		},
		{
			name:    "verbose mode shows endpoint",
			args:    []string{"threads", "list", "--verbose"},
			threads: []model.Thread{thread1},
			wantErrOut: []string{"Threads"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetCmdFlags(t)
			ts := setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Write(threadsJSON(tt.threads...))
			})
			_ = ts

			cap := captureAll(t, func() {
				rootCmd.SetArgs(tt.args)
				rootCmd.Execute()
			})

			if tt.wantExit && cap.Stderr == "" {
				t.Error("expected error output but got none")
			}
			for _, want := range tt.wantOut {
				if !strings.Contains(cap.Stdout, want) {
					t.Errorf("stdout missing %q\nstdout: %s", want, cap.Stdout)
				}
			}
			for _, want := range tt.wantErrOut {
				if !strings.Contains(cap.Stderr, want) {
					t.Errorf("stderr missing %q\nstderr: %s", want, cap.Stderr)
				}
			}
		})
	}
}

// TestThreadsListJSON verifies JSON output shape.
func TestThreadsListJSON(t *testing.T) {
	resetCmdFlags(t)
	thread := model.Thread{ID: 42, Type: "User", Name: "Admin", State: "Run", ElapsedTime: 5.0}
	ts := setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(threadsJSON(thread))
	})
	_ = ts

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"threads", "list", "--output", "json"})
		rootCmd.Execute()
	})

	var result []model.Thread
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\noutput: %s", err, out)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 thread, got %d", len(result))
	}
	if result[0].ID != 42 {
		t.Errorf("ID = %d, want 42", result[0].ID)
	}
	if result[0].State != "Run" {
		t.Errorf("State = %q, want Run", result[0].State)
	}
}

// TestThreadsListAll verifies --all disables the 50-row default cap.
func TestThreadsListAll(t *testing.T) {
	resetCmdFlags(t)
	threads := make([]model.Thread, 60)
	for i := range threads {
		threads[i] = model.Thread{ID: int64(i + 1), Name: "user", State: "Idle"}
	}
	ts := setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(threadsJSON(threads...))
	})
	_ = ts

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"threads", "list", "--all"})
		rootCmd.Execute()
	})

	if strings.Contains(out, "Showing 50 of 60") {
		t.Error("--all should not truncate to 50, but output suggests it did")
	}
	if !strings.Contains(out, "Showing 60 of 60") {
		t.Errorf("expected 'Showing 60 of 60' in output, got: %s", out)
	}
}
