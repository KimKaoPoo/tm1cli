package model

// ErrorLogFile is one entry from GET /ErrorLogFiles, the server-global
// collection of TM1 process error-log files. TM1 versions differ on the
// filename property name (Filename is the primary, TM1py-proven spelling;
// File is a fallback); LogFile returns whichever is set.
//
// Per-process error-log entries on Processes('name')/ErrorLogs are keyed by
// Timestamp and do NOT carry a filename — filenames live here. The canonical
// filename shape is TM1ProcessError_<14-digit-ts>_<processname>.log, which
// the command layer parses to derive the start time and to filter to a
// specific process.
type ErrorLogFile struct {
	Filename string `json:"Filename,omitempty"`
	File     string `json:"File,omitempty"`
}

// LogFile returns the filename, preferring Filename over File.
func (f ErrorLogFile) LogFile() string {
	if f.Filename != "" {
		return f.Filename
	}
	return f.File
}

// ErrorLogFileResponse is the OData collection wrapper for GET /ErrorLogFiles.
type ErrorLogFileResponse struct {
	Value []ErrorLogFile `json:"value"`
}

// ProcessHistoryEntry is the presentation shape for one process run row.
// User and Duration are always empty because the ErrorLogFiles endpoint
// does not expose them; JSON keeps them as "" (machine data) while the
// table renderer substitutes "-".
type ProcessHistoryEntry struct {
	StartTime    string `json:"start_time"`
	User         string `json:"user"`
	Duration     string `json:"duration"`
	Status       string `json:"status"`
	ErrorLogFile string `json:"error_log_file"`
}
