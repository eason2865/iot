# Contributing

Thanks for taking the time to improve IoT Platform.

## Development Setup

Install the Go toolchain specified in `go.mod`, then verify the repository:

```bash
go test ./...
```

You can also use the Makefile:

```bash
make fmt-check
make test
make build
```

The opt-in MQTT end-to-end tests use a running four-service deployment. Set
`IOT_E2E_API_URL` (default `http://127.0.0.1:8080`) and `MANAGEMENT_API_TOKEN`
for its management gateway, and point `EMQX_URL`, `KAFKA_BROKERS`, `POSTGRES_DSN`,
and `TDENGINE_DSN` at the same test stack. Use `IOT_E2E=1` for the single-device
flow or `IOT_E2E_LOAD=1` for five tenants and fifty devices.

Local validation artifacts belong in `outputs/`. Its nested Go module keeps
archived runners out of the project's package scan. To rebuild a standalone
runner with the project dependencies, run `go build -o /tmp/runner ./outputs/<case>/main.go` from the repository root.

## Testing operations diagnostics

Run `go test -race ./internal/opsagent` and `go test ./internal/platform -run TestCommandReadCredential` when changing a diagnostic tool or credential boundary. Exercise the opt-in deployment with `helm template iot charts/iot --set opsAgent.enabled=true`. Update both language editions of the architecture and command lifecycle diagrams when a flow changes.

## Commit Messages

Use `type: description` or `type(scope): description`. Choose `feat` for a new feature and `fix` for a bug fix. Other common types include `refactor`, `perf`, `docs`, `test`, `build`, `ci`, and `chore`. Use a short description that names the change.

For example, `refactor(worker): remove duplicate Kafka initialization` describes a cleanup, and `ci: authenticate protoc release lookup` describes a workflow change. Mark a breaking change with `!` before the colon or a `BREAKING CHANGE:` footer explaining the impact. [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/)

## Pull Request Checklist

- Keep changes focused on one topic.
- Run `gofmt` on Go files.
- Run `go test ./...`.
- Update documentation when behavior, configuration, API contracts, or deployment steps change.
- Do not commit local secrets, `.env` files, generated binaries, or `ai_handover.md`.

## Reporting Issues

When filing an issue, include:

- What you expected to happen.
- What happened instead.
- How to reproduce it.
- Relevant logs, configuration, and component names.

For security-sensitive reports, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.

Database regressions run when `IOT_POSTGRES_TEST_DSN` is set. Use a dedicated test
PostgreSQL database with permission to create schemas; every test creates and
drops its own schema. CI supplies PostgreSQL 16 and also runs race, vet, Helm
profiles and protobuf generation checks. Run `go test -race ./internal/...` locally
when changing concurrent code. `make build` stops immediately on a failed binary.
