# Migration Copilot — MVP Project Spec

## Goal
Build an embeddable Python SDK that generates a **plan-only** migration package for relational DB → warehouse migrations (initially Postgres → Snowflake/BigQuery), with explicit assumptions, confidence, and auditability.

## MVP Scope
**In scope**
- Parse minimal user intent into structured `MigrationSpec`.
- Use pluggable adapters to discover metadata and (optionally) design validations.
- Resolve missing constraints with policy-driven defaults.
- Produce a deterministic migration plan DAG.
- Generate artifact bundle (**JSON + Markdown + templates**) without executing migrations.
- Emit audit trail of rules fired and rationale.

**Out of scope (MVP)**
- Owning orchestration/scheduling.
- Secret management.
- Full HTTP platform (optional thin wrapper later).
- Automatic live migration execution.

## Primary Inputs
- `source` / `target` systems.
- Objects to migrate (tables; later extensible to topics/buckets).
- Optional downtime SLO (e.g., `< 5 min`).
- Optional constraints: throughput, cost/compliance, allowed tooling.
- Discovery data via adapters: schema graph, sizes, row counts, CDC readiness signals, optional metrics.

## Primary Outputs
1. **ResolvedSpec**: fully populated constraints, assumptions, confidence, open questions.
2. **MigrationPlan**: DAG stages (backfill → sync/CDC → validation gates → cutover → rollback criteria).
3. **ArtifactBundle**:
   - connector/config templates,
   - SQL/dbt skeletons,
   - validation SQL/check definitions,
   - Markdown cutover runbook.
4. **AuditTrail**: decisions, rules fired, risk/mitigations, evidence used.

## Architecture (Sequential Agent Pipeline)
Each agent returns **machine-readable JSON** + **human summary**.

1. **IntakeSpecBuilder**
   - Input: user text + optional partial spec.
   - Output: `MigrationSpec` (with defaults: policy=`conservative`, mode=`plan_only`, downtime=`unknown` if absent).
2. **DiscoveryProfiler**
   - Input: `MigrationSpec`, `MetadataAdapter`, optional `MetricsAdapter`.
   - Output: `SourceProfile`, `TargetProfile` (PK/FK/cycles, sizes, CDC readiness, DQ signals).
3. **ConstraintResolver**
   - Input: spec + profiles + policy profile.
   - Output: `ResolvedSpec` (inferred constraints, assumptions, confidence, confirmations needed).
4. **StrategyPlanner**
   - Input: `ResolvedSpec` + profiles.
   - Output: `MigrationPlan` (pattern + per-table strategy + gates + rollback triggers).
5. **ArtifactGenerator**
   - Input: `MigrationPlan` + dialect/tooling constraints.
   - Output: `ArtifactBundle` templates + runbook.
6. **ValidationDesigner**
   - Input: plan + optional validation adapter.
   - Output: `ValidationPlan` (+ optional `ValidationReport` if runnable).
7. **ExplainerAuditor**
   - Input: all prior outputs.
   - Output: `AuditLog` + executive summary.

## SDK Integration Model
- Python-first package.
- Host provides adapters; copilot never owns secrets/jobs.
- Required adapter: `MetadataAdapter`.
- Optional adapters: `ValidationAdapter`, `MetricsAdapter`.

Minimal adapter interfaces:
- `MetadataAdapter`: schemas, PK/FK metadata, table stats, change-rate hints.
- `ValidationAdapter` (optional): query execution for checks.
- `MetricsAdapter` (optional): lag/throughput/error metrics.

## Policy Profiles
- **conservative** (default): safer assumptions, stricter validation/cutover gates.
- **balanced**: moderate risk/performance tradeoff.
- **fast**: speed-prioritized; lower default verification strictness.

## Determinism & Safety Requirements
- Deterministic outputs for same input + discovery snapshot.
- Every inferred value must include rationale and confidence.
- Explicit “unknowns / confirm with team” list is mandatory.
- No destructive operations in MVP.

## Schemas (MVP Deliverables)
Define JSON Schemas for:
- `MigrationSpec`
- `ResolvedSpec`
- `MigrationPlan`
- `ArtifactBundleManifest`
- `AuditLog`

## File Structure
```text
migration_copilot/
  sdk/
    engine/                # agent orchestration + contracts
    policies/              # conservative/balanced/fast rules
    adapters/              # protocols/interfaces + reference stubs
    schemas/               # JSON schemas
    templates/             # SQL/dbt/config/runbook templates
    explain/               # audit + summaries
  examples/
    postgres_to_snowflake/
  tests/
    unit/
    golden/                # deterministic output snapshots
```

## MVP Acceptance Criteria
- Given minimal intent (`source`, `target`, tables, optional downtime), SDK returns:
  - valid `ResolvedSpec`,
  - valid `MigrationPlan` DAG,
  - generated artifact bundle (JSON + Markdown + templates),
  - `AuditTrail` with rule provenance.
- Works with mocked adapters and one reference relational→warehouse adapter fixture.
- Snapshot tests verify deterministic planning output.
