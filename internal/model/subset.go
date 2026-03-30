package model

type Subset struct {
	Name string `json:"Name"`
}

type SubsetResponse struct {
	Value []Subset `json:"value"`
}
