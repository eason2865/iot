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
