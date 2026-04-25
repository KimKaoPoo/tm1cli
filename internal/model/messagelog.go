package model

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

// MessageLogResponse is the OData collection wrapper for GET /MessageLogEntries.
type MessageLogResponse struct {
	Value []MessageLogEntry `json:"value"`
}
