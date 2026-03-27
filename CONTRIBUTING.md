# Contributing to tm1cli

Thanks for your interest in contributing! Here's how to get started.

## Development Setup

1. **Fork** the repo and clone your fork
2. Install Go 1.22+
3. Build and test:

```bash
make build
make test
```

## Workflow

1. Create a branch from `main`:
   ```bash
   git checkout -b feature/your-feature
   ```
2. Make your changes
3. Run tests:
   ```bash
   make test
   ```
4. Commit with a clear message:
   ```bash
   git commit -m "feat: add cube export to xlsx"
   ```
5. Push and open a PR against `main`

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

| Prefix | Use |
|--------|-----|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `docs:` | Documentation only |
| `refactor:` | Code change that doesn't fix a bug or add a feature |
| `test:` | Adding or updating tests |
| `chore:` | Build, CI, or tooling changes |

## Code Guidelines

- Follow standard Go conventions (`gofmt`, `go vet`)
- Write table-driven tests
- Use `httptest.NewServer` for API mocking — no real server credentials in tests
- Keep the CLI output consistent with existing commands

## Reporting Bugs

Use the [Bug Report](https://github.com/KimKaoPoo/tm1cli/issues/new?template=bug_report.md) template.

## Requesting Features

Use the [Feature Request](https://github.com/KimKaoPoo/tm1cli/issues/new?template=feature_request.md) template.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
