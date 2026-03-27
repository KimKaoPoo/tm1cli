package config

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	DefaultLimit      = 50
	DefaultOutput     = "table"
	DefaultShowSystem = false
	DefaultTLSVerify  = false
)

type ServerConfig struct {
	URL       string `json:"url"`
	User      string `json:"user"`
	Password  string `json:"password"`
	AuthMode  string `json:"auth_mode"`
	Namespace string `json:"namespace,omitempty"`
}

type Settings struct {
	DefaultLimit int    `json:"default_limit"`
	OutputFormat string `json:"output_format"`
	ShowSystem   bool   `json:"show_system"`
	TLSVerify    bool   `json:"tls_verify"`
}

type Config struct {
	Default  string                  `json:"default"`
	Settings Settings                `json:"settings"`
	Servers  map[string]ServerConfig `json:"servers"`
}

func DefaultSettings() Settings {
	return Settings{
		DefaultLimit: DefaultLimit,
		OutputFormat: DefaultOutput,
		ShowSystem:   DefaultShowSystem,
		TLSVerify:    DefaultTLSVerify,
	}
}

func configDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("cannot determine home directory: %w", err)
	}
	return filepath.Join(home, ".tm1cli"), nil
}

func ConfigPath() (string, error) {
	dir, err := configDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "config.json"), nil
}

func Load() (*Config, error) {
	path, err := ConfigPath()
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("cannot read config: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("Config file corrupted. Run 'tm1cli config add' to create a new one, or fix %s manually", path)
	}

	if cfg.Servers == nil {
		cfg.Servers = make(map[string]ServerConfig)
	}
	if cfg.Settings.DefaultLimit == 0 {
		cfg.Settings.DefaultLimit = DefaultLimit
	}
	if cfg.Settings.OutputFormat == "" {
		cfg.Settings.OutputFormat = DefaultOutput
	}

	return &cfg, nil
}

func Save(cfg *Config) error {
	dir, err := configDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("cannot create config directory: %w", err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot marshal config: %w", err)
	}

	path, err := ConfigPath()
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

func NewConfig() *Config {
	return &Config{
		Settings: DefaultSettings(),
		Servers:  make(map[string]ServerConfig),
	}
}

func EncodePassword(password string) string {
	return base64.StdEncoding.EncodeToString([]byte(password))
}

func DecodePassword(encoded string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("cannot decode password: %w", err)
	}
	return string(data), nil
}

func (c *Config) GetServer(name string) (*ServerConfig, error) {
	if name == "" {
		name = c.Default
	}
	if name == "" {
		return nil, fmt.Errorf("No connection configured. Run 'tm1cli config add' first.")
	}
	srv, ok := c.Servers[name]
	if !ok {
		return nil, fmt.Errorf("Connection '%s' not found. Run 'tm1cli config list' to see available.", name)
	}
	return &srv, nil
}

func (c *Config) GetEffectivePassword(serverName string) (string, error) {
	if envPass := os.Getenv("TM1CLI_PASSWORD"); envPass != "" {
		return envPass, nil
	}
	srv, err := c.GetServer(serverName)
	if err != nil {
		return "", err
	}
	return DecodePassword(srv.Password)
}

func (c *Config) AddServer(name string, srv ServerConfig) {
	c.Servers[name] = srv
	if c.Default == "" || len(c.Servers) == 1 {
		c.Default = name
	}
}

func (c *Config) RemoveServer(name string) string {
	delete(c.Servers, name)
	if c.Default == name {
		c.Default = ""
		for n := range c.Servers {
			c.Default = n
			break
		}
	}
	return c.Default
}

func (c *Config) GetEffectiveOutput() string {
	if envOutput := os.Getenv("TM1CLI_OUTPUT"); envOutput != "" {
		return envOutput
	}
	if c.Settings.OutputFormat != "" {
		return c.Settings.OutputFormat
	}
	return DefaultOutput
}

func (c *Config) GetEffectiveServer() string {
	if envServer := os.Getenv("TM1CLI_SERVER"); envServer != "" {
		return envServer
	}
	return c.Default
}
