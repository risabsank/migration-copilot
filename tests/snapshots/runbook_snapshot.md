# Migration Runbook

Pattern: **backfill_plus_cdc**
Selected plan variant: **backfill_cdc_sync**
Confidence: **0.9**
AI-first planner: **disabled**

## AI Multi-Agent Notes
- No AI agent notes captured.

## Available Plan Variants
- backfill_cdc_sync
- batch_only

## Steps
- **prepare** (prepare) — depends on: none. Prepare migration

## Step-by-Step Backfill
1. Backfill **users** in chunks of **1000** rows.

## CDC Sync Plan
- CDC readiness: **ready**
- Log mode: **wal**
- Lag gate: replication lag <= **30s**
- Stabilization window: **10 minutes**
- Reprocessing strategy: replay
- Dedupe strategy: pk

## Sync + Validation Gates
- Start CDC/incremental sync after initial backfill.
- Gate 1: replication lag remains <= 30s for 10 minutes.
- Gate 2: validation queries in `validations.sql` show zero critical deltas.

## Estimate
- Duration: **30 minutes**
- Peak workers: **4**
- Compute credits: **1.2**

## Compliance Gates
- SOX: PASS — ok

## Cutover Checklist
- Freeze schema changes in source system.
- Confirm latest validation run is successful.
- Redirect reads and writes to target.
- Monitor error rate and data freshness for the first hour.

## Rollback Criteria
- validation drift > 0.1%

## Confirm With Team
- Approve cutover window
