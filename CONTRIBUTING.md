# Contributing

Thanks for taking the time to improve IoT Platform.

## Development Setup

Install Go 1.25 or newer, then verify the repository:

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
