package model

// Sandbox represents a TM1 sandbox.
//
// Field names match the tm1.Sandbox OData entity returned by GET /Sandboxes
// (verified against the TM1py reference client). The Loaded/Active/Queued
// flags are exposed by TM1 as Is-prefixed booleans.
type Sandbox struct {
	Name                      string `json:"Name"`
	IncludeInSandboxDimension bool   `json:"IncludeInSandboxDimension"`
	IsLoaded                  bool   `json:"IsLoaded"`
	IsActive                  bool   `json:"IsActive"`
	IsQueued                  bool   `json:"IsQueued"`
}

// SandboxResponse is the OData collection wrapper for GET /Sandboxes.
type SandboxResponse struct {
	Value []Sandbox `json:"value"`
}
