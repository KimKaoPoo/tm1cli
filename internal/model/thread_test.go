package model

import (
	"encoding/json"
	"testing"
)

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
		got := ParseODataDuration(tt.input)
		if got != tt.want {
			t.Errorf("ParseODataDuration(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestThreadDurationUnmarshalJSON(t *testing.T) {
	t.Run("float64 value", func(t *testing.T) {
		var d ThreadDuration
		if err := json.Unmarshal([]byte("5.0"), &d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if float64(d) != 5.0 {
			t.Errorf("got %v, want 5.0", d)
		}
	})
	t.Run("ISO duration string", func(t *testing.T) {
		var d ThreadDuration
		if err := json.Unmarshal([]byte(`"PT10S"`), &d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if float64(d) != 10.0 {
			t.Errorf("got %v, want 10.0", d)
		}
	})
}
