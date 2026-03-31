package model

type View struct {
	Name string `json:"Name"`
}

type ViewResponse struct {
	Value []View `json:"value"`
}
