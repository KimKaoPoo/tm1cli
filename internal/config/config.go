package config

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	DefaultLimit      = 50
	DefaultOutput     = "table"
	DefaultShowSystem = false
	DefaultTLSVerify  = false

	SourceEnv    = "env"
	SourceLocal  = "local"
	SourceGlobal = "global"

	PasswordStorageBase64   = "base64"
	PasswordStorageKeychain = "keychain"
)

type ServerConfig struct {
	URL             string `json:"url"`
	User            string `json:"user"`
	Password        string `json:"password"`                    // set when storage is "" or "base64"
	PasswordStorage string `json:"password_storage,omitempty"` // "", "base64", or "keychain"
	PasswordRef     string `json:"password_ref,omitempty"`     // keychain account (random 128-bit hex)
	AuthMode        string `json:"auth_mode"`
	Namespace       string `json:"namespace,omitempty"`
}

type Settings struct {
	DefaultLimit int    `json:"default_limit"`
	OutputFormat string `json:"output_format"`
	ShowSystem   bool   `json:"show_system"`
	TLSVerify    bool   `json:"tls_verify"`
}

type Config struct {
	Default    string                  `json:"default"`
	Settings   Settings                `json:"settings"`
	Servers    map[string]ServerConfig `json:"servers"`
	loadedFrom string // file path that was loaded; not serialized
	source     string // "env", "local", or "global"; not serialized
}

func (c *Config) ConfigSource() string { return c.source }
func (c *Config) LoadedFrom() string   { return c.loadedFrom }
func (c *Config) IsLocalConfig() bool  { return c.source == SourceLocal }

func DefaultSettings() Settings {
	return Settings{
		DefaultLimit: DefaultLimit,
		OutputFormat: DefaultOutput,
		ShowSystem:   DefaultShowSystem,
		TLSVerify:    DefaultTLSVerify,
	}
}

func globalConfigDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("cannot determine home directory: %w", err)
	}
	return filepath.Join(home, ".tm1cli"), nil
}

func globalConfigPath() (string, error) {
	dir, err := globalConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "config.json"), nil
}

// resolveConfigPath determines which config file to use based on precedence:
// 1. TM1CLI_CONFIG env var  2. Local .tm1cli/config.json (walk upward)  3. Global ~/.tm1cli/config.json
func resolveConfigPath() (path string, source string, err error) {
	if envPath := os.Getenv("TM1CLI_CONFIG"); envPath != "" {
		return envPath, SourceEnv, nil
	}
	gp, err := globalConfigPath()
	if err != nil {
		return "", "", err
	}
	if local := findLocalConfig(gp); local != "" {
		return local, SourceLocal, nil
	}
	return gp, SourceGlobal, nil
}

// findLocalConfig walks from cwd upward looking for .tm1cli/config.json.
// It explicitly excludes globalPath to avoid misclassifying the global config as local.
func findLocalConfig(globalPath string) string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		candidate := filepath.Join(dir, ".tm1cli", "config.json")
		if candidate != globalPath {
			if _, err := os.Stat(candidate); err == nil {
				return candidate
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

func Load() (*Config, error) {
	path, source, err := resolveConfigPath()
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

	cfg.loadedFrom = path
	cfg.source = source
	return &cfg, nil
}

func Save(cfg *Config) error {
	path := cfg.loadedFrom
	if path == "" {
		resolved, source, err := resolveConfigPath()
		if err != nil {
			return err
		}
		path = resolved
		cfg.loadedFrom = path
		cfg.source = source
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("cannot create config directory: %w", err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot marshal config: %w", err)
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

func newPasswordRef() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// StorePassword tries to store the password in the OS keychain; falls back to base64.
// Mutates srv fields (Password, PasswordStorage, PasswordRef).
// Returns usedKeychain and a warning string (empty if keychain succeeded).
func StorePassword(srv *ServerConfig, password string) (usedKeychain bool, warning string) {
	ref := srv.PasswordRef
	if ref == "" {
		ref = newPasswordRef()
	}
	err := SetKeychainPassword(ref, password)
	if err == nil {
		srv.Password = ""
		srv.PasswordStorage = PasswordStorageKeychain
		srv.PasswordRef = ref
		return true, ""
	}
	// Keychain write failed — clean up any pre-existing keychain entry for
	// this server so it doesn't linger as an orphan after we switch to
	// base64 storage. Best-effort: the keychain may still be unreachable.
	if srv.PasswordStorage == PasswordStorageKeychain && srv.PasswordRef != "" {
		_ = DeleteKeychainPassword(srv.PasswordRef)
	}
	srv.Password = EncodePassword(password)
	srv.PasswordStorage = PasswordStorageBase64
	srv.PasswordRef = ""
	return false, fmt.Sprintf("keychain unavailable (%v), password stored base64-encoded in config file", err)
}

// ClearStoredPassword removes the keychain entry if the server uses keychain storage.
// No-op for base64 storage.
func ClearStoredPassword(srv *ServerConfig) error {
	if srv.PasswordStorage == PasswordStorageKeychain && srv.PasswordRef != "" {
		return DeleteKeychainPassword(srv.PasswordRef)
	}
	return nil
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
	if serverName == "" {
		serverName = c.Default
	}
	srv, err := c.GetServer(serverName)
	if err != nil {
		return "", err
	}
	if srv.PasswordStorage == PasswordStorageKeychain {
		if srv.PasswordRef == "" {
			return "", fmt.Errorf("server '%s' marked as keychain-stored but has no credential reference; run 'tm1cli config edit %s' to re-enter", serverName, serverName)
		}
		pw, err := GetKeychainPassword(srv.PasswordRef)
		if errors.Is(err, ErrKeychainNotFound) {
			return "", fmt.Errorf("password for '%s' not found in OS keychain. Run 'tm1cli config edit %s' to re-enter", serverName, serverName)
		}
		if err != nil {
			return "", fmt.Errorf("cannot read password from keychain for '%s': %w", serverName, err)
		}
		return pw, nil
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
