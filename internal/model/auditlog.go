package model

// AuditLogEntry represents a single entry from GET /AuditLogEntries.
// Fields follow the TM1 v11 REST schema for tm1.AuditLogEntry.
//
// ID is Edm.String per IBM docs; older TM1 versions may omit it (empty == absent).
// AuditDetails is optional and not always populated; it carries free-form
// per-object change details when present.
type AuditLogEntry struct {
	ID           string `json:"ID,omitempty"`
	TimeStamp    string `json:"TimeStamp"`
	User         string `json:"User,omitempty"`
	ObjectType   string `json:"ObjectType,omitempty"`
	ObjectName   string `json:"ObjectName,omitempty"`
	Description  string `json:"Description,omitempty"`
	AuditDetails string `json:"AuditDetails,omitempty"`
}

// AuditLogResponse is the OData collection wrapper for GET /AuditLogEntries.
// NextLink is set when the response is paginated; callers must surface it.
type AuditLogResponse struct {
	Value    []AuditLogEntry `json:"value"`
	NextLink string          `json:"@odata.nextLink,omitempty"`
}
