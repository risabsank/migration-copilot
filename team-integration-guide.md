# Team Integration Guide

This guide explains how an engineering team can work with this repository, extend it safely, and embed migration planning output into their own platform.

## 1) What this project does (and does not do)

`migration-copilot` is a **plan generator**. It inspects source metadata, selects a migration strategy, and emits artifacts your team can run in your own execution stack.

It does:
- Generate deterministic migration plans.
- Generate runbook and SQL/YAML scaffolding.
- Emit auditable events for planning steps.

It does **not**:
- Execute migrations in production.
- Manage your secrets/connector infra.
- Replace orchestration, deployment, or observability platforms.

## 2) Core team workflows

### Workflow A: Product/data team requests a migration plan

1. Create or update a spec (JSON or YAML).
2. Run `migration-copilot plan --spec ... --out ...`.
3. Review generated `plan.json` and `runbook.md` in PR.
4. Approve and hand off generated SQL/config templates to execution owners.

### Workflow B: Platform team adds support for a new source

1. Implement a metadata adapter that satisfies `MetadataAdapter`.
2. Add tests for table discovery and table description semantics.
3. Validate generated plans with snapshot tests.
4. Document adapter setup + caveats in your internal docs.

### Workflow C: Governance/compliance review

1. Inspect `events.jsonl` and `runbook.md` for deterministic gates and risks.
2. Verify policy profile and downtime constraints in input spec.
3. Require signed review before running generated templates.

## 3) Repository map for contributors

- `sdk/cli.py`: CLI surface (`plan`, `doctor`, `init-spec`).
- `sdk/copilot.py`: main façade and end-to-end planning flow.
- `sdk/engine/`: strategy selection, validation, rule engine, model contracts.
- `sdk/adapters/`: adapter protocols + implementations.
- `sdk/artifacts/`: output bundle generation.
- `sdk/observability.py`: event model and log writer.
- `tests/`: behavior, snapshot, adapter contract, governance tests.
- `examples/`: reference specs and orchestration examples.

## 4) How to integrate in your own codebase

### Option 1: Use CLI in CI/CD (lowest effort)

Use this when you want a stable planning job without writing Python integration code.

```bash
migration-copilot plan --spec ./migration-spec.json --out ./artifacts/migration-plan
```

Recommended CI contract:
- Upload the full artifact directory as a pipeline artifact.
- Publish `runbook.md` and `plan.json` as build annotations.
- Fail pipeline if `doctor` fails.

### Option 2: Use SDK in application/service code

Use this when you need custom adapter logic and programmatic control over plan lifecycle.

```python
from sdk.copilot import MigrationCopilot
from sdk.engine.models import MigrationSpec, PolicyProfile

copilot = MigrationCopilot(metadata_adapter=my_adapter)

spec = MigrationSpec(
    source_type="postgres",
    target_type="snowflake",
    objects=["orders", "customers"],
    downtime_minutes=5,
    policy_profile=PolicyProfile.CONSERVATIVE,
)

output = copilot.plan(spec=spec, schema="public", output_dir="./artifacts/plan-a")
```

Recommended integration boundaries:
- Keep planner invocation in a dedicated service/module.
- Treat generated artifacts as immutable build outputs.
- Send plan metadata (`plan_id`, confidence, selected variant) to your observability system.

## 5) Adapter implementation standards

A custom adapter must provide:
- `list_tables(schema: str) -> list[str]`
- `describe_table(table_name: str, schema: str) -> TableMetadata`

Guidelines for robust adapters:
- Return deterministic ordering where possible.
- Clamp/normalize invalid estimates before returning metadata.
- Populate FK relationships accurately; DAG quality depends on this.
- Add graceful handling for unsupported table features (log + explicit fallback behavior).

Recommended adapter test matrix:
- Empty schema.
- Large table with no PK.
- Circular/complex FK graph.
- Mixed nullable/non-nullable columns.

## 6) Team ownership model

A practical ownership split:

- **Data Platform Team**
  - Owns adapters and execution bridge.
  - Maintains generated SQL execution jobs.
- **Application Teams**
  - Own table selection and cutover windows.
  - Review business-risk and rollback criteria.
- **Governance/SRE**
  - Own approval gates, alerts, and runbook compliance.

Use CODEOWNERS (in your host repo) to enforce reviews:
- Adapter changes → platform reviewers.
- Rules/policy changes → governance reviewers.
- Artifact template changes → platform + app reviewers.

## 7) Suggested PR checklist for this repo (or downstream forks)

- [ ] Plan remains deterministic for same spec + metadata.
- [ ] New/changed behavior covered by tests.
- [ ] Snapshot changes reviewed intentionally.
- [ ] Runbook language updated where behavior changed.
- [ ] Backward compatibility of spec fields considered.

## 8) Operationalizing outputs in a real migration program

Treat the generated bundle as input to staged environments:

1. **Dev**: validate SQL templates compile and run against masked datasets.
2. **Staging**: run full backfill + CDC simulation + cutover rehearsal.
3. **Prod**: execute in waves using runbook gates.

Minimum production gates to implement externally:
- Row-count and checksum deltas below threshold.
- CDC lag within SLO for stabilization window.
- Rollback switch documented and tested.
- Observability dashboard with lag, errors, and freshness.

## 9) Versioning and upgrade policy for teams

When consuming this project from your own repository:
- Pin a package/tag version instead of floating `main`.
- Capture generated artifact diffs when upgrading versions.
- Re-run snapshot tests for representative specs after upgrades.

Recommended internal process:
1. Upgrade in a branch.
2. Re-plan 3–5 representative migrations.
3. Compare `plan.json`, `runbook.md`, and validation SQL diffs.
4. Approve with platform + governance sign-off.

## 10) Fast onboarding runbook for new team members

Day 1 checklist:
1. Install package in editable mode.
2. Run plan on `examples/spec.json`.
3. Inspect generated artifacts locally.
4. Run tests.
5. Read `sdk/copilot.py` and `sdk/engine/models.py` to understand object model.

Starter commands:

```bash
python -m pip install -e .[dev]
python -m sdk.cli doctor --spec examples/spec.json
python -m sdk.cli plan --spec examples/spec.json --out ./artifacts/onboarding
PYTHONPATH=. pytest -q
```

---

If your team forks this project, keep this guide in your fork and customize sections 5–9 with your environment specifics (warehouse, orchestration stack, governance model, and release process).
