package model

// Session represents an active session on the TM1 server.
//
// Fields match the tm1.Session OData entity: ID/Context/Active are direct
// properties; User and Threads are navigation properties exposed via
// $expand. (TM1 does NOT expose a LastActivity property on Session.)
type Session struct {
	ID      int64           `json:"ID"`
	Context string          `json:"Context"`
	Active  bool            `json:"Active"`
	User    SessionUser     `json:"User"`
	Threads []SessionThread `json:"Threads"`
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
//
// Count holds @odata.count when the request used $count=true and the
// server honored it. Pointer so absence is distinguishable from zero.
type SessionResponse struct {
	Value []Session `json:"value"`
	Count *int64    `json:"@odata.count,omitempty"`
}

// ActiveSessionRef captures the ID of the current authenticated session
// returned by GET /ActiveSession.
type ActiveSessionRef struct {
	ID int64 `json:"ID"`
}
