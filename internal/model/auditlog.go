package model

// AuditLogEntry represents a single entry from GET /AuditLogEntries.
// Fields follow the TM1 PA REST schema for tm1.AuditLogEntry — note the
// audit log uses UserName (not User as in TransactionLogEntry); the two
// entity types diverge here.
//
// ID is Edm.String per IBM docs; older TM1 versions may omit it (empty
// == absent). AuditDetails is a navigation property in the schema and
// only populates with $expand=AuditDetails — not requested here, so it
// is intentionally not modelled.
type AuditLogEntry struct {
	ID          string `json:"ID,omitempty"`
	TimeStamp   string `json:"TimeStamp"`
	UserName    string `json:"UserName,omitempty"`
	ObjectType  string `json:"ObjectType,omitempty"`
	ObjectName  string `json:"ObjectName,omitempty"`
	Description string `json:"Description,omitempty"`
}

// AuditLogResponse is the OData collection wrapper for GET /AuditLogEntries.
// NextLink is set when the response is paginated; callers must surface it.
type AuditLogResponse struct {
	Value    []AuditLogEntry `json:"value"`
	NextLink string          `json:"@odata.nextLink,omitempty"`
}
