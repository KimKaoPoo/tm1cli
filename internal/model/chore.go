package model

type Chore struct {
	Name           string      `json:"Name"`
	Active         bool        `json:"Active"`
	StartTime      string      `json:"StartTime"`
	DSTSensitivity bool        `json:"DSTSensitivity"`
	Frequency      string      `json:"Frequency"`
	Tasks          []ChoreTask `json:"Tasks,omitempty"`
}

type ChoreResponse struct {
	Value []Chore `json:"value"`
}

type ChoreTask struct {
	Step       int              `json:"Step"`
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
