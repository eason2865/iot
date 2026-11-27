# Security Policy

## Supported Versions

Security updates are provided for the latest released version.

| Version | Supported |
| --- | --- |
| 3.0 | Yes |

## Operations agent credentials

Issue a separate operator token and single-command read credential for each tenant. Store model credentials in the dedicated agent Secret. A configured model receives the operator’s question and sanitized evidence. Treat its output as advice. Report cross-tenant access, evidence leaks or any ability to write through the agent through the channel below.

## Reporting a Vulnerability

Please do not open public issues for security vulnerabilities.

Report security issues privately to the repository owner or maintainer. Include:

- A clear description of the vulnerability.
- Steps to reproduce it.
- Affected components and versions.
- Any suggested mitigation if available.

The maintainers will acknowledge the report, assess impact, and coordinate a fix before public disclosure when appropriate.
