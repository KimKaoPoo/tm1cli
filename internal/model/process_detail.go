package model

// ProcessDetail represents a full TI process definition for serialization.
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

// ProcessParamDef represents a TI process parameter definition.
// Note: Value is interface{} because TM1 returns string or numeric values.
// YAML roundtrip may decode numeric values as int instead of float64;
// this is benign because json.Marshal produces identical wire format for both.
type ProcessParamDef struct {
	Name   string      `json:"Name" yaml:"name"`
	Prompt string      `json:"Prompt" yaml:"prompt"`
	Value  interface{} `json:"Value" yaml:"value"`
	Type   string      `json:"Type" yaml:"type"`
}

// ProcessDataSource represents a TI process data source configuration.
type ProcessDataSource struct {
	Type                    string `json:"Type" yaml:"type"`
	AsciiDecimalSeparator   string `json:"asciiDecimalSeparator,omitempty" yaml:"ascii_decimal_separator,omitempty"`
	AsciiDelimiterChar      string `json:"asciiDelimiterChar,omitempty" yaml:"ascii_delimiter_char,omitempty"`
	AsciiDelimiterType      string `json:"asciiDelimiterType,omitempty" yaml:"ascii_delimiter_type,omitempty"`
	AsciiHeaderRecords      *int   `json:"asciiHeaderRecords,omitempty" yaml:"ascii_header_records,omitempty"`
	AsciiQuoteCharacter     string `json:"asciiQuoteCharacter,omitempty" yaml:"ascii_quote_character,omitempty"`
	AsciiThousandSeparator  string `json:"asciiThousandSeparator,omitempty" yaml:"ascii_thousand_separator,omitempty"`
	DataSourceNameForClient string `json:"dataSourceNameForClient,omitempty" yaml:"data_source_name_for_client,omitempty"`
	DataSourceNameForServer string `json:"dataSourceNameForServer,omitempty" yaml:"data_source_name_for_server,omitempty"`
}

// ProcessVariable represents a TI process variable definition.
type ProcessVariable struct {
	Name      string `json:"Name" yaml:"name"`
	Type      string `json:"Type" yaml:"type"`
	StartByte int    `json:"StartByte" yaml:"start_byte"`
	EndByte   int    `json:"EndByte" yaml:"end_byte"`
}
