package model

import "encoding/json"

// TransactionLogEntry represents a single entry from GET /TransactionLogEntries.
//
// OldValue and NewValue are TM1 Edm.PrimitiveType — they may arrive as JSON
// null, number, string, or bool — so they ride as json.RawMessage and are
// formatted at render time. Modelling them as string would fail unmarshal
// for non-string responses.
//
// ID is Edm.Int64; older TM1 versions may omit it (zero == absent).
type TransactionLogEntry struct {
	ID            int64           `json:"ID,omitempty"`
	TimeStamp     string          `json:"TimeStamp"`
	ChangeSetID   string          `json:"ChangeSetID,omitempty"`
	User          string          `json:"User"`
	Cube          string          `json:"Cube"`
	Tuple         []string        `json:"Tuple"`
	OldValue      json.RawMessage `json:"OldValue"`
	NewValue      json.RawMessage `json:"NewValue"`
	StatusMessage string          `json:"StatusMessage,omitempty"`
}

// TransactionLogResponse is the OData collection wrapper.
type TransactionLogResponse struct {
	Value []TransactionLogEntry `json:"value"`
}
