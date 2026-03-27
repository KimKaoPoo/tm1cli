package model

type Dimension struct {
	Name string `json:"Name"`
}

type DimensionResponse struct {
	Value []Dimension `json:"value"`
}

type Element struct {
	Name string `json:"Name"`
	Type string `json:"Type"`
}

type ElementResponse struct {
	Value []Element `json:"value"`
}
