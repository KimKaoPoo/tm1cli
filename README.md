# tm1cli

[![Release](https://img.shields.io/github/v/release/KimKaoPoo/tm1cli)](https://github.com/KimKaoPoo/tm1cli/releases/latest)
[![Downloads](https://img.shields.io/github/downloads/KimKaoPoo/tm1cli/total)](https://github.com/KimKaoPoo/tm1cli/releases)
[![Go](https://img.shields.io/github/go-mod/go-version/KimKaoPoo/tm1cli)](https://github.com/KimKaoPoo/tm1cli/blob/main/go.mod)
[![License](https://img.shields.io/github/license/KimKaoPoo/tm1cli)](https://github.com/KimKaoPoo/tm1cli/blob/main/LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/KimKaoPoo/tm1cli/ci.yml)](https://github.com/KimKaoPoo/tm1cli/actions/workflows/ci.yml)

A command-line tool for IBM TM1 / Planning Analytics REST API.

Manage TM1 servers, list cubes and dimensions, run TI processes, and export data — all from the terminal.

## Features

- **Multi-server config** — save and switch between TM1 server connections
- **Cubes & Dimensions** — list, filter, and browse cube structures and dimension members
- **TI Processes** — list and execute TurboIntegrator processes with parameters
- **Data Export** — export cube views to screen, CSV, JSON, or XLSX
- **Flexible output** — table or JSON format, with filtering and pagination
- **Secure** — no hardcoded credentials; supports environment variable overrides

## Install

### Homebrew (macOS / Linux)

```bash
brew install KimKaoPoo/tap/tm1cli
```

Or:

```bash
brew tap KimKaoPoo/tap
brew install tm1cli
```

### Download binary

Download the latest release for your platform from [Releases](https://github.com/KimKaoPoo/tm1cli/releases/latest).

**macOS (Apple Silicon):**

```bash
curl -Lo tm1cli.tar.gz https://github.com/KimKaoPoo/tm1cli/releases/latest/download/tm1cli_0.1.2_darwin_arm64.tar.gz
tar xzf tm1cli.tar.gz
sudo mv tm1cli /usr/local/bin/
```

**macOS (Intel):**

```bash
curl -Lo tm1cli.tar.gz https://github.com/KimKaoPoo/tm1cli/releases/latest/download/tm1cli_0.1.2_darwin_amd64.tar.gz
tar xzf tm1cli.tar.gz
sudo mv tm1cli /usr/local/bin/
```

**Linux (x86_64):**

```bash
curl -Lo tm1cli.tar.gz https://github.com/KimKaoPoo/tm1cli/releases/latest/download/tm1cli_0.1.2_linux_amd64.tar.gz
tar xzf tm1cli.tar.gz
sudo mv tm1cli /usr/local/bin/
```

**Linux (ARM64):**

```bash
curl -Lo tm1cli.tar.gz https://github.com/KimKaoPoo/tm1cli/releases/latest/download/tm1cli_0.1.2_linux_arm64.tar.gz
tar xzf tm1cli.tar.gz
sudo mv tm1cli /usr/local/bin/
```

**Windows:**

Download `tm1cli_0.1.2_windows_amd64.zip` from [Releases](https://github.com/KimKaoPoo/tm1cli/releases/latest), extract, and add `tm1cli.exe` to your PATH.

### Go install

```bash
go install github.com/KimKaoPoo/tm1cli@latest
```

### From source

Requires Go 1.22+

```bash
git clone https://github.com/KimKaoPoo/tm1cli.git
cd tm1cli
make build
```

The binary is built to `./tm1cli`. Optionally install to PATH:

```bash
make install   # copies to /usr/local/bin
```

## Quick Start

```bash
# Add your TM1 server connection (interactive)
tm1cli config add myserver

# List cubes
tm1cli cubes

# List dimensions
tm1cli dims

# Browse dimension members
tm1cli dims members Period

# Run a TI process
tm1cli process run "LoadData" --param pSource=file.csv

# Export a cube view
tm1cli export "Sales" --view "Default"
```

## Configuration

Connections are stored in `~/.tm1cli/config.json`.

```bash
tm1cli config add myserver          # add a connection (interactive)
tm1cli config list                  # list all connections
tm1cli config use production        # switch active connection
tm1cli config remove old_server     # remove a connection
tm1cli config settings              # view default settings
tm1cli config settings --limit 100  # change defaults
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `TM1CLI_SERVER` | Override active connection |
| `TM1CLI_OUTPUT` | Override output format (`table` or `json`) |
| `TM1CLI_PASSWORD` | Override stored password (recommended for CI/CD) |

### Password Security

Passwords are stored in the OS keychain (macOS Keychain, Linux secret-service/libsecret, Windows Credential Manager). If the keychain is unavailable (e.g., headless Linux without D-Bus), tm1cli falls back to base64-encoded storage in the config file and prints a warning — base64 is obfuscation only, not encryption.

For CI/CD or headless environments, prefer the `TM1CLI_PASSWORD` environment variable.

**Config file portability:** The `password_ref` in the config file is a machine-local keychain lookup key. Copying `~/.tm1cli/config.json` to another machine will not copy the passwords — re-enter them there via `tm1cli config edit <name>`. Keychain entries are scoped to the OS user account.

**Migrating existing base64 passwords:** Existing configs with base64-stored passwords continue to work unchanged. To move an individual connection into the keychain, run `tm1cli config edit <name>` and re-enter your password at the prompt.

## Usage

### Cubes

```bash
tm1cli cubes                        # list cubes (default limit: 50)
tm1cli cubes --filter "ledger"      # filter by name
tm1cli cubes --all                  # show all (no limit)
tm1cli cubes --show-system          # include system cubes (} prefix)
tm1cli cubes --count                # count only
tm1cli cubes --output json          # JSON output
```

### Dimensions & Members

```bash
tm1cli dims                                # list dimensions
tm1cli dims members Period                 # list elements
tm1cli dims members Region --hierarchy "Alternate Region"
tm1cli dims members Account --filter "Rev"
```

### Processes

```bash
tm1cli process list                        # list TI processes
tm1cli process list --filter "load"
tm1cli process run "LoadData"              # run without params
tm1cli process run "LoadData" --param pSource=file.csv --param pYear=2024
```

### Export

```bash
tm1cli export "Sales" --view "Default"               # print table to screen
tm1cli export "Sales" --view "Default" -o data.csv   # write CSV file
tm1cli export "Sales" --view "Default" -o data.json  # write JSON file
tm1cli export "Sales" --view "Default" -o report.xlsx # write Excel file
tm1cli export "Sales" --view "Default" --output json  # JSON to screen
tm1cli export "Sales" --view "Default" -o data.csv --no-header  # CSV without header
```

### Global Flags

```
--server <name>   Use a specific connection
--output <format> Output format: table or json
--verbose         Show request details
--version         Print version
```

## Auth Modes

| Mode | TM1 Security Mode | Usage |
|------|-------------------|-------|
| `basic` | Mode 1 (TM1 native) | `--auth basic` |
| `cam` | Mode 4/5 (CAM/LDAP) | `--auth cam --namespace <ns>` |

## Roadmap

- [x] v0.1.0 — Config, cubes, dims, members, process list/run, export view → table
- [x] v0.1.1 — Export view → CSV/JSON file
- [ ] v0.2.0 — MDX export, XLSX output, config edit, CAM auth testing
- [ ] v0.3.0 — tab completion, advanced features
- [ ] v0.4.0 — OS keychain password storage

## License

[MIT](LICENSE)
