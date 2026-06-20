package model

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// MessageLogEntry represents a single entry in the TM1 message log.
// User is omitempty because older TM1 versions may not include the field.
type MessageLogEntry struct {
	ID        string `json:"ID,omitempty"`
	TimeStamp string `json:"TimeStamp"`
	Logger    string `json:"Logger,omitempty"`
	Level     string `json:"Level"`
	Message   string `json:"Message"`
	User      string `json:"User,omitempty"`
}

// UnmarshalJSON accepts both representations of the OData Edm.Int64 ID.
// TM1 normally emits a JSON number, while IEEE754-compatible OData responses
// and some older versions emit a quoted decimal string.
func (e *MessageLogEntry) UnmarshalJSON(data []byte) error {
	var wire struct {
		ID        json.RawMessage `json:"ID"`
		TimeStamp string          `json:"TimeStamp"`
		Logger    string          `json:"Logger"`
		Level     string          `json:"Level"`
		Message   string          `json:"Message"`
		User      string          `json:"User"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}

	id, err := parseMessageLogID(wire.ID)
	if err != nil {
		return err
	}
	*e = MessageLogEntry{
		ID:        id,
		TimeStamp: wire.TimeStamp,
		Logger:    wire.Logger,
		Level:     wire.Level,
		Message:   wire.Message,
		User:      wire.User,
	}
	return nil
}

func parseMessageLogID(raw json.RawMessage) (string, error) {
	value := strings.TrimSpace(string(raw))
	if value == "" || value == "null" {
		return "", nil
	}
	if strings.HasPrefix(value, `"`) {
		var id string
		if err := json.Unmarshal(raw, &id); err != nil {
			return "", fmt.Errorf("cannot parse message log ID: %w", err)
		}
		return id, nil
	}
	if _, err := strconv.ParseInt(value, 10, 64); err != nil {
		return "", fmt.Errorf("cannot parse message log ID %q as Int64: %w", value, err)
	}
	return value, nil
}

// MessageLogResponse is the OData collection wrapper for GET /MessageLogEntries.
type MessageLogResponse struct {
	Value []MessageLogEntry `json:"value"`
}
