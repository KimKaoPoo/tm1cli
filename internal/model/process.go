package model

type Process struct {
	Name string `json:"Name"`
}

type ProcessResponse struct {
	Value []Process `json:"value"`
}

type ProcessParameter struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

type ProcessExecuteBody struct {
	Parameters []ProcessParameter `json:"Parameters,omitempty"`
}

type ProcessRunResult struct {
	Process    string `json:"process"`
	Status     string `json:"status"`
	DurationMs int64  `json:"duration_ms"`
	Message    string `json:"message"`
}
