# ADR 0003: Use Helm As The Kubernetes Manifest Source

## Status

Accepted

## Context

The repository previously had both Helm charts and `k8s/local` Kustomize manifests. Maintaining both caused duplicated deployment definitions and drift risk.

## Decision

Use `charts/iot` as the Kubernetes manifest source. Keep default values for application-only deployment with external dependencies. Use `charts/iot/values-local-stack.yaml` to reproduce the previous full local stack behavior.

## Consequences

- Kubernetes changes should be made in the Helm chart first.
- Local full-stack deployment should use `helm upgrade --install ... -f charts/iot/values-local-stack.yaml`.
- Namespace creation is handled by Helm commands with `--create-namespace`.
- The removed `k8s/local` directory should not be recreated unless there is a new explicit deployment strategy decision.
