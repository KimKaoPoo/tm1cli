package model

// Session represents an active session on the TM1 server.
type Session struct {
	ID           int64           `json:"ID"`
	Context      string          `json:"Context"`
	Active       bool            `json:"Active"`
	LastActivity string          `json:"LastActivity"`
	User         SessionUser     `json:"User"`
	Threads      []SessionThread `json:"Threads"`
}

// SessionUser is the expanded User navigation property on a Session.
type SessionUser struct {
	Name string `json:"Name"`
}

// SessionThread captures the minimum needed (ID) to count threads per session.
type SessionThread struct {
	ID int64 `json:"ID"`
}

// SessionResponse is the OData collection wrapper for GET /Sessions.
type SessionResponse struct {
	Value []Session `json:"value"`
}

// ActiveSessionRef captures the ID of the current authenticated session
// returned by GET /ActiveSession.
type ActiveSessionRef struct {
	ID int64 `json:"ID"`
}
