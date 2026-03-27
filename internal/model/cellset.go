package model

type CellsetCell struct {
	Value   interface{} `json:"Value"`
	Ordinal int         `json:"Ordinal"`
}

type CellsetAxis struct {
	Ordinal int              `json:"Ordinal"`
	Tuples  []CellsetTuple   `json:"Tuples"`
}

type CellsetTuple struct {
	Ordinal int              `json:"Ordinal"`
	Members []CellsetMember  `json:"Members"`
}

type CellsetMember struct {
	Name string `json:"Name"`
}

type ViewExecuteResponse struct {
	Axes  []CellsetAxis `json:"Axes"`
	Cells []CellsetCell `json:"Cells"`
}
