package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"tm1cli/internal/config"
	"tm1cli/internal/model"
)

// capturedOutput holds captured stdout and stderr.
type capturedOutput struct {
	Stdout string
	Stderr string
}

// captureStdout captures os.Stdout output during the execution of fn.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("cannot create pipe: %v", err)
	}

	origStdout := os.Stdout
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = origStdout

	var buf bytes.Buffer
	buf.ReadFrom(r)
	r.Close()

	return buf.String()
}

// captureStderr captures os.Stderr output during the execution of fn.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("cannot create pipe: %v", err)
	}

	origStderr := os.Stderr
	os.Stderr = w

	fn()

	w.Close()
	os.Stderr = origStderr

	var buf bytes.Buffer
	buf.ReadFrom(r)
	r.Close()

	return buf.String()
}

// captureAll captures both stdout and stderr during the execution of fn.
func captureAll(t *testing.T, fn func()) capturedOutput {
	t.Helper()

	origStdout := os.Stdout
	origStderr := os.Stderr

	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatalf("cannot create stdout pipe: %v", err)
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatalf("cannot create stderr pipe: %v", err)
	}

	os.Stdout = wOut
	os.Stderr = wErr

	fn()

	wOut.Close()
	wErr.Close()
	os.Stdout = origStdout
	os.Stderr = origStderr

	outBytes, _ := io.ReadAll(rOut)
	errBytes, _ := io.ReadAll(rErr)

	return capturedOutput{
		Stdout: string(outBytes),
		Stderr: string(errBytes),
	}
}

// setupMockTM1 creates a mock TM1 REST API server and a config file pointing to it.
// The handler receives HTTP requests as they would arrive at the TM1 server.
// Returns the httptest.Server (auto-closed via t.Cleanup).
func setupMockTM1(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	cfg := &config.Config{
		Default:  "test",
		Settings: config.DefaultSettings(),
		Servers: map[string]config.ServerConfig{
			"test": {
				URL:      ts.URL + "/api/v1",
				User:     "admin",
				Password: config.EncodePassword("testpass"),
				AuthMode: "basic",
			},
		},
	}
	writeTestConfig(t, cfg)
	return ts
}

// writeTestConfig writes a config to a temp HOME directory.
func writeTestConfig(t *testing.T, cfg *config.Config) {
	t.Helper()
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("TM1CLI_CONFIG", "")
	t.Chdir(tmpDir)

	cfgDir := filepath.Join(tmpDir, ".tm1cli")
	if err := os.MkdirAll(cfgDir, 0700); err != nil {
		t.Fatalf("cannot create config dir: %v", err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("cannot marshal config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cfgDir, "config.json"), data, 0600); err != nil {
		t.Fatalf("cannot write config: %v", err)
	}
}

// resetCmdFlags resets all package-level cobra flag variables to their zero/default values.
// Call this at the start of each integration test to avoid state leaking between tests.
func resetCmdFlags(t *testing.T) {
	t.Helper()
	zeroAllFlags()
	t.Cleanup(zeroAllFlags)
}

// zeroAllFlags resets every package-level flag variable to its zero value.
// When adding a new flag variable to any cmd/*.go file, add it here too —
// otherwise integration tests may leak state between runs.
func zeroAllFlags() {
	flagServer = ""
	flagOutput = ""
	flagVerbose = false
	cubesFilter = ""
	cubesLimit = 0
	cubesAll = false
	cubesShowSystem = false
	cubesCount = false
	dimsFilter = ""
	dimsLimit = 0
	dimsAll = false
	dimsShowSystem = false
	dimsCount = false
	membersHierarchy = ""
	membersFilter = ""
	membersLimit = 0
	membersAll = false
	membersCount = false
	procListFilter = ""
	procListLimit = 0
	procListAll = false
	procListShowSystem = false
	procListCount = false
	procRunParams = nil
	procDumpOut = ""
	procLoadFile = ""
	procLoadCreateOnly = false
	procLoadUpdateOnly = false
	exportView = ""
	exportMDX = ""
	exportOut = ""
	exportNoHeader = false
	viewsFilter = ""
	viewsLimit = 0
	viewsAll = false
	viewsCount = false
	subsetsFilter = ""
	subsetsLimit = 0
	subsetsAll = false
	subsetsCount = false
	subsetsHierarchy = ""
}

// cubesJSON returns JSON for a TM1 Cubes response.
func cubesJSON(cubes ...string) []byte {
	type cube struct {
		Name           string `json:"Name"`
		LastDataUpdate string `json:"LastDataUpdate,omitempty"`
	}
	resp := struct {
		Value []cube `json:"value"`
	}{}
	for _, name := range cubes {
		resp.Value = append(resp.Value, cube{Name: name, LastDataUpdate: "2024-01-15T10:30:00"})
	}
	data, _ := json.Marshal(resp)
	return data
}

// dimsJSON returns JSON for a TM1 Dimensions response.
func dimsJSON(dims ...string) []byte {
	type dim struct {
		Name string `json:"Name"`
	}
	resp := struct {
		Value []dim `json:"value"`
	}{}
	for _, name := range dims {
		resp.Value = append(resp.Value, dim{Name: name})
	}
	data, _ := json.Marshal(resp)
	return data
}

// elementsJSON returns JSON for a TM1 Elements response.
func elementsJSON(names []string, types []string) []byte {
	type elem struct {
		Name string `json:"Name"`
		Type string `json:"Type"`
	}
	resp := struct {
		Value []elem `json:"value"`
	}{}
	for i, name := range names {
		typ := "Numeric"
		if i < len(types) {
			typ = types[i]
		}
		resp.Value = append(resp.Value, elem{Name: name, Type: typ})
	}
	data, _ := json.Marshal(resp)
	return data
}

// viewsJSON returns JSON for a TM1 Views response.
func viewsJSON(names ...string) []byte {
	type view struct {
		Name string `json:"Name"`
	}
	resp := struct {
		Value []view `json:"value"`
	}{}
	for _, name := range names {
		resp.Value = append(resp.Value, view{Name: name})
	}
	data, _ := json.Marshal(resp)
	return data
}

// subsetsJSON returns JSON for a TM1 Subsets response.
func subsetsJSON(names ...string) []byte {
	type subset struct {
		Name string `json:"Name"`
	}
	resp := struct {
		Value []subset `json:"value"`
	}{}
	for _, name := range names {
		resp.Value = append(resp.Value, subset{Name: name})
	}
	data, _ := json.Marshal(resp)
	return data
}

// processesJSON returns JSON for a TM1 Processes response.
func processesJSON(procs ...string) []byte {
	type proc struct {
		Name string `json:"Name"`
	}
	resp := struct {
		Value []proc `json:"value"`
	}{}
	for _, name := range procs {
		resp.Value = append(resp.Value, proc{Name: name})
	}
	data, _ := json.Marshal(resp)
	return data
}

// activeUserJSON returns JSON for a TM1 ActiveUser response.
func activeUserJSON(name string) []byte {
	data, _ := json.Marshal(model.ActiveUser{Name: name})
	return data
}

// serverConfigJSON returns JSON for a TM1 Configuration response.
func serverConfigJSON(name, version, host string, port int) []byte {
	data, _ := json.Marshal(model.ServerConfiguration{
		ServerName:     name,
		ProductVersion: version,
		AdminHost:      host,
		HTTPPortNumber: port,
	})
	return data
}
