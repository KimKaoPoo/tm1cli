package model

// Sandbox represents a TM1 sandbox.
//
// Fields match the tm1.Sandbox OData entity returned by GET /Sandboxes.
type Sandbox struct {
	Name                      string `json:"Name"`
	IncludeInSandboxDimension bool   `json:"IncludeInSandboxDimension"`
	Loaded                    bool   `json:"Loaded"`
	Active                    bool   `json:"Active"`
	Queued                    bool   `json:"Queued"`
}

// SandboxResponse is the OData collection wrapper for GET /Sandboxes.
type SandboxResponse struct {
	Value []Sandbox `json:"value"`
}
