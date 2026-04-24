package model

import (
	"encoding/json"
	"strconv"
	"strings"
)

// ThreadDuration represents TM1 thread elapsed/wait time in seconds.
// TM1 may return a float64 (legacy) or an ISO 8601 / OData Edm.Duration string.
type ThreadDuration float64

func (d *ThreadDuration) UnmarshalJSON(b []byte) error {
	var f float64
	if err := json.Unmarshal(b, &f); err == nil {
		*d = ThreadDuration(f)
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*d = ThreadDuration(ParseODataDuration(s))
	return nil
}

// ParseODataDuration parses ISO 8601 / OData Edm.Duration strings to seconds.
// Handles: "PT10.5S", "PT1M30S", "PT1H0M10S", "duration'PT10S'"
// Day components (e.g. "P1DT10S") are not supported — the day digit is discarded.
// TM1 thread durations are always sub-day so this is not a practical concern.
func ParseODataDuration(s string) float64 {
	s = strings.TrimPrefix(s, "duration'")
	s = strings.TrimSuffix(s, "'")
	s = strings.TrimPrefix(s, "P")
	if idx := strings.Index(s, "T"); idx >= 0 {
		s = s[idx+1:]
	}
	var total float64
	for _, unit := range []struct {
		suffix string
		mult   float64
	}{
		{"H", 3600}, {"M", 60}, {"S", 1},
	} {
		if idx := strings.Index(s, unit.suffix); idx >= 0 {
			v, err := strconv.ParseFloat(s[:idx], 64)
			if err == nil { // malformed component contributes 0; intentional silent fallback
				total += v * unit.mult
			}
			s = s[idx+1:]
		}
	}
	return total
}

// Thread represents a running thread on the TM1 server.
type Thread struct {
	ID          int64          `json:"ID"`
	Type        string         `json:"Type"`
	Name        string         `json:"Name"`
	Context     string         `json:"Context"`
	State       string         `json:"State"`
	Function    string         `json:"Function"`
	ObjectType  string         `json:"ObjectType"`
	ObjectName  string         `json:"ObjectName"`
	RLocks      int            `json:"RLocks"`
	IXLocks     int            `json:"IXLocks"`
	WLocks      int            `json:"WLocks"`
	ElapsedTime ThreadDuration `json:"ElapsedTime"`
	WaitTime    ThreadDuration `json:"WaitTime"`
	Info        string         `json:"Info"`
}

// ThreadResponse is the OData collection wrapper for GET /Threads.
type ThreadResponse struct {
	Value []Thread `json:"value"`
}
