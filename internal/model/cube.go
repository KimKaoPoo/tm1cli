package model

type Cube struct {
	Name           string `json:"Name"`
	LastDataUpdate string `json:"LastDataUpdate,omitempty"`
}

type CubeResponse struct {
	Value []Cube `json:"value"`
}
