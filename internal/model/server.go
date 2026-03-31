package model

// ActiveUser represents the response from GET /ActiveUser.
type ActiveUser struct {
	Name string `json:"Name"`
}

// ServerConfiguration represents the response from GET /Configuration.
type ServerConfiguration struct {
	ServerName     string `json:"ServerName"`
	ProductVersion string `json:"ProductVersion"`
	AdminHost      string `json:"AdminHost"`
	HTTPPortNumber int    `json:"HTTPPortNumber"`
}
