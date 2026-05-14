package model

type Chore struct {
	Name         string      `json:"Name"`
	Active       bool        `json:"Active"`
	StartTime    string      `json:"StartTime"`
	DSTSensitive bool        `json:"DSTSensitive"`
	Frequency    string      `json:"Frequency"`
	Tasks        []ChoreTask `json:"Tasks,omitempty"`
}

type ChoreResponse struct {
	Value []Chore `json:"value"`
}

type ChoreTask struct {
	Step       int              `json:"Step,omitempty"`
	Process    ChoreTaskProcess `json:"Process"`
	Parameters []ChoreTaskParam `json:"Parameters"`
}

type ChoreTaskProcess struct {
	Name string `json:"Name"`
}

type ChoreTaskParam struct {
	Name  string      `json:"Name"`
	Value interface{} `json:"Value"`
}

// ChoreWithTasks is the projection used by --verbose preflight.
type ChoreWithTasks struct {
	Name  string      `json:"Name"`
	Tasks []ChoreTask `json:"Tasks"`
}

// ChoreRunResult is the JSON payload emitted by `chores run`.
// Status values: "completed", "error", "timeout", "started".
type ChoreRunResult struct {
	Chore      string `json:"chore"`
	Status     string `json:"status"`
	DurationMs int64  `json:"duration_ms"`
	ThreadID   string `json:"thread_id,omitempty"`
	Timeout    string `json:"timeout,omitempty"`
	Message    string `json:"message,omitempty"`
}
