package model

// ProcessErrorLog is one entry from GET /Processes('name')/ErrorLogs.
// TM1 versions differ on the filename property: Filename is the primary
// (TM1py-proven) spelling, File a fallback; LogFile returns whichever is set.
// The Timestamp json tag also absorbs the "TimeStamp" spelling via
// encoding/json's case-insensitive field matching.
type ProcessErrorLog struct {
	Timestamp   string `json:"Timestamp"`
	ProcessName string `json:"ProcessName,omitempty"`
	Filename    string `json:"Filename,omitempty"`
	File        string `json:"File,omitempty"`
}

// LogFile returns the error-log filename, preferring Filename over File.
func (p ProcessErrorLog) LogFile() string {
	if p.Filename != "" {
		return p.Filename
	}
	return p.File
}

// ProcessErrorLogResponse is the OData collection wrapper for
// GET /Processes('name')/ErrorLogs.
type ProcessErrorLogResponse struct {
	Value []ProcessErrorLog `json:"value"`
}

// ProcessHistoryEntry is the presentation shape for one process run row.
// User and Duration are always empty when sourced from the ErrorLogs
// endpoint (it does not expose them); JSON keeps them as "" (machine data)
// while the table renderer substitutes "-".
type ProcessHistoryEntry struct {
	StartTime    string `json:"start_time"`
	User         string `json:"user"`
	Duration     string `json:"duration"`
	Status       string `json:"status"`
	ErrorLogFile string `json:"error_log_file"`
}
