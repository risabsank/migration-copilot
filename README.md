# Migration Copilot

Migration Copilot is a **deterministic migration planning SDK** for relational-to-warehouse modernization programs.

It helps teams convert migration intent into a reproducible artifact bundle (plan, runbook, SQL templates, CDC config stubs, governance outputs, and event logs) that can be reviewed, versioned, and executed in your existing delivery platform.

---

## Why Migration Copilot

Large-scale data migrations often fail due to inconsistent planning, unclear ownership, and non-repeatable execution prep. Migration Copilot addresses this by providing:

- **Deterministic planning**: same spec + metadata yields the same planning output.
- **Auditability**: generated `events.jsonl` and governance artifacts support operational traceability.
- **Reusable scaffolding**: produces templates for backfill, validation, CDC, transforms, and orchestration handoff.
- **Extensibility**: adapter contracts let you plug in custom metadata sources and runtime integrations.

---

## Core capabilities

- Facade API (`MigrationCopilot`) to build migration plans from a `MigrationSpec`.
- CLI workflows for day-1 to day-2 operations:
  - `init-spec`: bootstrap a starter migration spec.
  - `doctor`: run preflight checks on spec and artifact readiness.
  - `plan`: generate a complete migration artifact bundle.
- Static metadata adapter for JSON/YAML-driven planning in CI and local environments.
- Postgres metadata adapter reference implementation.
- Multi-agent planning pathway with deterministic fallback behavior.
- Operator control plane API + lightweight web UI for plan monitoring and lifecycle control.

---

## Architecture at a glance

1. **Specification intake** (`MigrationSpec` via JSON/YAML or SDK).
2. **Metadata discovery** (adapter contract).
3. **Constraint and dependency analysis** (rule engine + planning models).
4. **Variant selection** (strategy and confidence scoring).
5. **Artifact packaging** (SQL, runbook, CDC, DAG scaffolds, governance signatures/checksums).
6. **Execution handoff** (integrate into your orchestrator, connector stack, and CI/CD).

---

## Installation

### Requirements

- Python 3.10+
- pip

### Install

```bash
python -m pip install -e .
```

For YAML spec support (if needed in other environments):

```bash
python -m pip install "migration-copilot[yaml]"
```

---

## Quickstart (CLI)

### 1) Create a starter spec

```bash
migration-copilot init-spec --template postgres_snowflake --out ./examples/my-spec.json
```

### 2) Validate the setup and spec

```bash
migration-copilot doctor --spec ./examples/my-spec.json
```

### 3) Generate a migration bundle

```bash
migration-copilot plan --spec ./examples/my-spec.json --out ./artifacts/demo
```

Expected outputs include:

- `plan.json`
- `runbook.md`
- `validations.sql`
- `backfill/*.sql`
- `transforms/stg_*.sql`
- `cdc/*.yaml`
- `dags/*.py`
- `governance/*`
- `events.jsonl`

---

<<<<<<< HEAD
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
=======
## SDK usage (embedded in your platform)
>>>>>>> 9cf7b28 (updated README)

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

output = copilot.plan(
    spec=spec,
    schema="public",
    cdc_supported=True,
    cdc_log_mode="wal",
    output_dir="./artifacts/my-plan",
)

print(output.result.as_dict())
print(output.runbook_markdown)
```

---

## Reusability and extension model

Migration Copilot is designed as a planning layer you can adapt across teams and migration programs.

### 1) Adapter extensibility

Implement `MetadataAdapter` for your source system:

- `list_tables(schema: str) -> list[str]`
- `describe_table(table_name: str, schema: str) -> TableMetadata`

This allows reuse with any catalog/metadata source (live DB, schema registry, exported manifests, etc.).

### 2) Orchestrator integration

Generated artifact bundles can be consumed by:

- Airflow
- Dagster
- dbt-driven flows
- Internal workflow engines

Use generated DAG and SQL files as scaffolding, then replace placeholders with production logic.

### 3) Policy-driven rollout

Tune execution posture via `policy_profile` and operational constraints (`downtime_minutes`, `cdc_supported`, etc.) to reuse the same framework for conservative and aggressive migration tracks.

### 4) Governance and promotion

Bundle signatures/checksums support gated promotion from development to staging to production in CI/CD.

---

## LLM / planning configuration

Migration Copilot can use an OpenAI-compatible endpoint when configured:

- `MIGRATION_COPILOT_LLM_ENDPOINT`
- `MIGRATION_COPILOT_LLM_API_KEY`
- `MIGRATION_COPILOT_LLM_MODEL` (default behavior falls back when not configured)

If unset, deterministic planning fallback paths remain available.

---

<<<<<<< HEAD
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
=======
## Operator control plane

Run the control plane API locally:
>>>>>>> 9cf7b28 (updated README)

```bash
python -m sdk.control_plane.api
```

<<<<<<< HEAD
Then open `http://127.0.0.1:8000/`. See `docs/operator-gui.md` for details.
=======
Then open:

- `http://127.0.0.1:8000/`

See `docs/operator-gui.md` for workflow details.

---

## Project structure

- `sdk/`
  - `engine/` - planning models, rule engine, validation, agent interfaces
  - `adapters/` - adapter contracts and implementations
  - `artifacts/` - bundle assembly and artifact emitters
  - `execution/` - cutover, backfill, rollback, validation helpers
  - `orchestration/` - policy, supervisor, service/scheduler integration
  - `control_plane/` - API service and web assets
  - `state/`, `operations/`, `connectors/` - runtime state and ops integrations
- `examples/` - sample specs and generation helpers
- `artifacts/demo/` - sample generated bundle
- `docs/` - deployment, local execution, adapter and operator guides

---

## Development workflow

Run baseline checks:

```bash
ruff check .
PYTHONPATH=. pytest -q
```

Optional type checking:

```bash
mypy --explicit-package-bases sdk/state sdk/adapters sdk/observability.py tests/test_contracts.py tests/test_output_snapshots.py
```

---

## Production adoption notes

Migration Copilot is intentionally strongest at **planning and packaging**. For full production migration execution, pair it with:

- Managed secrets and key rotation
- CDC connector provisioning and monitoring
- Data quality and reconciliation execution pipelines
- Incident response automation and rollback operations
- Environment-specific SLO/SLA monitoring and alerting
>>>>>>> 9cf7b28 (updated README)
