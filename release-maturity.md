# Release Maturity

## Current Stage

Spooky is currently in **Beta**.

Beta means:
- Core proxying, routing, load balancing, and health-check features are implemented and actively validated.
- Controlled production rollout is supported with strict observability, staged traffic, and rollback plans.
- The project remains pre-GA; operational hardening and long-duration validation are still in progress.

## Environment Guidance

- **Development**: Fully supported.
- **Staging**: Fully supported.
- **Production**: Supported for controlled rollout and bounded blast radius deployments.

## Not Yet GA

The following categories remain required for GA promotion:
- Extended soak validation under sustained production-like load.
- Broader failure-mode validation and runbook hardening.
- Continued performance and allocation optimization for high-scale routing paths.
- Stability window with no critical regressions across release cycles.

## Operator Expectations In Beta

- Roll out gradually (canary or segmented traffic).
- Enforce SLO-based alerting before increasing traffic share.
- Keep rollback paths tested and immediately available.
- Revisit release notes and roadmap on each upgrade.

## Related Docs

- [Roadmap](roadmap.md)
- [Production Deployment](deployment/production.md)
- [Troubleshooting](troubleshooting/common-issues.md)
