package model

import (
	"encoding/json"
	"testing"
)

func TestErrorLogFile_LogFile(t *testing.T) {
	tests := []struct {
		name string
		f    ErrorLogFile
		want string
	}{
		{name: "filename only", f: ErrorLogFile{Filename: "b.log"}, want: "b.log"},
		{name: "file only", f: ErrorLogFile{File: "a.log"}, want: "a.log"},
		{name: "both set prefers filename", f: ErrorLogFile{Filename: "b.log", File: "a.log"}, want: "b.log"},
		{name: "neither set", f: ErrorLogFile{}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.f.LogFile(); got != tt.want {
				t.Errorf("LogFile() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestErrorLogFileResponse_Unmarshal(t *testing.T) {
	raw := []byte(`{"value":[{"Filename":"TM1ProcessError_20260528090402_Load.log"}]}`)

	var resp ErrorLogFileResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if len(resp.Value) != 1 {
		t.Fatalf("len(Value) = %d, want 1", len(resp.Value))
	}
	if got := resp.Value[0].LogFile(); got != "TM1ProcessError_20260528090402_Load.log" {
		t.Errorf("LogFile() = %q", got)
	}
}
