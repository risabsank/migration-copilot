# Migration Copilot

A Python SDK that generates **deterministic, plan-only** migration bundles for relational database to warehouse migrations.

The SDK discovers table metadata through adapters, resolves constraints, builds a migration plan, and writes artifacts (plan JSON, runbook, SQL templates, CDC templates, and an event log).

## Project overview

### What this repository currently provides

- `MigrationCopilot` facade API to generate migration plans from a `MigrationSpec` and metadata adapter.
- CLI command (`migration-copilot plan`) that consumes a JSON/YAML spec file and emits a bundle.
- Reference Postgres metadata adapter.
- AI-first multi-agent planner with deterministic fallback heuristic.
- Artifact generator for runbooks, validation SQL, backfill SQL stubs, dbt transform stubs, and CDC configs.

### High-level flow

1. Build `MigrationSpec`.
2. Discover metadata from a `MetadataAdapter`.
3. Resolve constraints and pick strategy pattern.
4. Build migration DAG + rollback criteria.
5. Generate artifact bundle.
6. Emit auditable event log (`events.jsonl`).

## Quickstart

### Prerequisites

- Python 3.10+
- `pip`

### Install for local development

```bash
python -m pip install -e .
```

### Generate a migration plan from example spec

```bash
python -m sdk.cli plan --spec examples/spec.json --out ./artifacts/demo
```

You should get a JSON summary on stdout and these generated outputs under `./artifacts/demo`:

- `plan.json`
- `runbook.md`
- `validations.sql`
- `backfill/*.sql`
- `transforms/stg_*.sql`
- `cdc/*.yaml`
- `events.jsonl`

## Run with a real Postgres source

`main.py` uses environment variables and the Postgres metadata adapter.

```bash
export DATABASE_URL='postgresql://user:pass@localhost:5432/mydb'
export TARGET_TYPE='snowflake'
export TABLES='orders,customers'
export SOURCE_SCHEMA='public'
export DOWNTIME_MINUTES='5'
export CDC_LOG_MODE='wal'
python main.py
```

If `DATABASE_URL` is missing, the script exits early with guidance.

## Running tests and checks

### Current test command

```bash
ruff check .
mypy --explicit-package-bases sdk/state sdk/adapters sdk/observability.py tests/test_contracts_hardening.py tests/test_output_snapshots.py
PYTHONPATH=. pytest -q
```

Snapshot-specific checks:

```bash
PYTHONPATH=. pytest -q tests/test_snapshots.py tests/test_output_snapshots.py tests/test_contracts_hardening.py
```

## Using this SDK in your own project

### 1) Add dependency

If publishing/packaging is desired, depend on this package by git URL or local path. For local integration:

```bash
python -m pip install /path/to/migration-copilot
```

### 2) Implement a metadata adapter (or reuse Postgres adapter)

Your adapter must implement:

- `list_tables(schema: str) -> list[str]`
- `describe_table(table_name: str, schema: str) -> TableMetadata`

### 3) Create and run the copilot

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

output = copilot.plan(spec=spec, schema="public", output_dir="./artifacts/my-plan")
print(output.result.as_dict())
print(output.runbook_markdown)
```

### 4) Integrate generated artifacts into your runtime/orchestration

This project only plans and scaffolds migration artifacts. You still need to wire generated SQL/YAML/templates into your execution platform (Airflow, Dagster, dbt, connector tooling, CI/CD, etc.).

## What is still missing to run this as a full migration solution

This repository intentionally focuses on planning and scaffolding. To run migrations end-to-end in production, you still need:

1. **Execution/orchestration layer** (e.g., Airflow/Dagster jobs).
2. **Secrets and connector configuration management**.
3. **Production CDC connector wiring** (`cdc/*.yaml` contains TODO placeholders).
4. **Automated test suite** (unit + snapshot tests are referenced in spec docs but not present).
5. **CI pipeline and quality gates** for linting/tests/snapshots.
6. **Schema contracts and validation execution plumbing** (currently only SQL templates are generated).
7. **Operational monitoring + alerting integration** (lag/error SLO dashboards and alerts).

## Optional LLM configuration

The planner can call an OpenAI-compatible endpoint if configured:

- `MIGRATION_COPILOT_LLM_ENDPOINT`
- `MIGRATION_COPILOT_LLM_API_KEY`
- `MIGRATION_COPILOT_LLM_MODEL` (default: `gpt-4o-mini`)

Without these, the code automatically uses a deterministic heuristic fallback client.

## Repository layout

- `sdk/`: core package
  - `engine/`: planning logic and models
  - `adapters/`: adapter contracts and Postgres adapter
  - `artifacts/`: bundle generation
- `examples/`: sample specs and generator examples
- `main.py`: env-driven example entrypoint
- `scope.md`: project scope and MVP spec

## Production-readiness docs

- Local execution: `docs/local-execution.md`
- Production deployment model: `docs/production-deployment-model.md`
- Adapter implementation guide: `docs/adapter-implementation-guide.md`
- Operational lifecycle + recovery: `docs/operational-lifecycle-and-recovery.md`

## Operator GUI

A lightweight operator UI is available via a stdlib HTTP API + React shell:

```bash
python -m sdk.control_plane.api
```

Then open `http://127.0.0.1:8000/`. See `docs/operator-gui.md` for details.