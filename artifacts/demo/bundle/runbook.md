# Migration Runbook

Pattern: **backfill_plus_cdc**
Selected plan variant: **backfill_cdc_sync**
Confidence: **0.98**
AI-first planner: **enabled**

## AI Multi-Agent Notes
- strategy_agent: Heuristic strategy output.
- risk_agent: Heuristic risk agent output.
- review_agent: approved

## Available Plan Variants
- batch_only
- backfill_cdc_sync
- phased_cutover_by_domain_or_table_group

## Steps
- **prepare** (prepare) — depends on: none. Freeze schema contracts and configure connections.
- **backfill** (backfill) — depends on: prepare. Backfill tables in execution order with configured chunk sizes.
- **sync** (sync) — depends on: backfill. Run incremental sync/CDC until lag gates pass.
- **validate** (validation) — depends on: sync. Run row-count, aggregate, checksum, and FK integrity checks.
- **cutover** (cutover) — depends on: validate. Switch reads/writes to target after all validation gates pass.
- **phased-cutover-domains** (cutover) — depends on: cutover. Execute cutover in domain/table-group waves with per-wave validation gates.

## Step-by-Step Backfill
1. Backfill **customers** in chunks of **3000000** rows.
2. Backfill **orders** in chunks of **3000000** rows.

## CDC Sync Plan
- CDC readiness: **ready**
- Log mode: **wal**
- Lag gate: replication lag <= **90s**
- Stabilization window: **45 minutes**
- Reprocessing strategy: Replay from checkpoint watermark and re-run idempotent upsert window.
- Dedupe strategy: Merge on primary key with event timestamp/version tie-breaker.

## Sync + Validation Gates
- CDC is optional for this pattern; run incrementals only if needed.
- Validation gate: all `validations.sql` checks must pass before cutover.

## Phased Cutover Groups
- Wave 1: orders
- Wave 2: customers

## Estimate
- Duration: **15 minutes**
- Peak workers: **6**
- Compute credits: **0.1**

## Compliance Gates
- SOX: PASS — Change control and rollback criteria are present.
- PII: PASS — PII-like table names require masking policy approval.
- retention: PASS — Retention policy checkpoint included in runbook.

## Cutover Checklist
- Freeze schema changes in source system.
- Confirm latest validation run is successful.
- Redirect reads and writes to target.
- Monitor error rate and data freshness for the first hour.

## Rollback Criteria
- Abort if critical validation checks fail.
- Abort if replication lag does not converge before cutover window.
- Abort if any schema compatibility score is below 0.8.
- Abort if schema drift introduces incompatible DDL.

## Confirm With Team
- None