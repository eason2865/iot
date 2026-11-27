# ADR 0011 · Read-only operations agent

Accepted in 3.0. Extends the application deployment boundary in [ADR 0003](0003-helm-only-kubernetes-manifests.md) and the tenant authorization model in [ADR 0010](0010-canonical-api-parameters-and-command-results.md).

## Decision

Deploy `ops-agent` as an optional independent Go service. Its operator credential selects a tenant. A separate tenant-scoped reader calls the existing command detail endpoint. The agent first verifies the command identity, then queries two fixed observability tools in parallel. Prometheus reports aggregate worker scrape availability. Tempo supplies the command's sampled stages. The management status is the authoritative finding.

A configured model receives the question and allowlisted evidence through a Chat Completions endpoint. Its answer is a separate advisory field. Tool selection, authorization, URLs, timeouts and result limits are enforced in Go. Tool calls do not require MCP. The agent has no Kubernetes service account token, business database connection, shell executor or command-publishing capability.

## Operational consequences

Release and scale the agent independently. An unavailable agent, trace backend or model cannot block device commands. Operator responses distinguish missing observations from upstream failures. Existing device ACKs, Kafka events, SQL schema and public management routes continue to work. See the [API contract](../api-contract.md#ops-agent-30) and [production guide](../production-deployment.md#operations-agent-30).

Release 3.1 extends the agent with authenticated Grafana intake and a dedicated notification queue. Only the queue role writes its own job table. The agent still has no access to the business database or device write tools. See the [alert deployment guide](../production-deployment.md#automatic-grafana-alert-analysis).
