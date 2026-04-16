package model

type ProcessDetail struct {
	Name              string            `json:"Name" yaml:"name"`
	PrologProcedure   string            `json:"PrologProcedure" yaml:"prolog"`
	MetadataProcedure string            `json:"MetadataProcedure" yaml:"metadata"`
	DataProcedure     string            `json:"DataProcedure" yaml:"data"`
	EpilogProcedure   string            `json:"EpilogProcedure" yaml:"epilog"`
	Parameters        []ProcessParamDef `json:"Parameters" yaml:"parameters"`
	DataSource        ProcessDataSource `json:"DataSource" yaml:"datasource"`
	Variables         []ProcessVariable `json:"Variables" yaml:"variables"`
}

type ProcessParamDef struct {
	Name   string      `json:"Name" yaml:"name"`
	Prompt string      `json:"Prompt" yaml:"prompt"`
	Value  interface{} `json:"Value" yaml:"value"`
	Type   string      `json:"Type" yaml:"type"`
}

type ProcessDataSource struct {
	Type                    string `json:"Type" yaml:"type"`
	DataSourceNameForServer string `json:"dataSourceNameForServer,omitempty" yaml:"dataSourceNameForServer,omitempty"`
	DataSourceNameForClient string `json:"dataSourceNameForClient,omitempty" yaml:"dataSourceNameForClient,omitempty"`
	ASCIIDecimalSeparator   string `json:"asciiDecimalSeparator,omitempty" yaml:"asciiDecimalSeparator,omitempty"`
	ASCIIDelimiterChar      string `json:"asciiDelimiterChar,omitempty" yaml:"asciiDelimiterChar,omitempty"`
	ASCIIDelimiterType      string `json:"asciiDelimiterType,omitempty" yaml:"asciiDelimiterType,omitempty"`
	ASCIIHeaderRecords      int    `json:"asciiHeaderRecords,omitempty" yaml:"asciiHeaderRecords,omitempty"`
	ASCIIQuoteCharacter     string `json:"asciiQuoteCharacter,omitempty" yaml:"asciiQuoteCharacter,omitempty"`
	ASCIIThousandSeparator  string `json:"asciiThousandSeparator,omitempty" yaml:"asciiThousandSeparator,omitempty"`
}

type ProcessVariable struct {
	Name      string `json:"Name" yaml:"name"`
	Type      string `json:"Type" yaml:"type"`
	Position  int    `json:"Position" yaml:"position"`
	StartByte int    `json:"StartByte" yaml:"startByte"`
	EndByte   int    `json:"EndByte" yaml:"endByte"`
}
