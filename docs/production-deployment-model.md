# Operational Lifecycle and Failure Recovery

This guide maps lifecycle phases to operational expectations and recovery actions.

## Lifecycle phases

`MigrationRun` captures current state and orchestration phase:

1. `drafted` / `plan_ready`
2. `approved`
3. `provisioning`
4. `backfilling`
5. `validating`
6. `syncing` + CDC catch-up
7. `cutover_ready`
8. `cutover_complete` or rollback path

## Failure classes

### Validation failure

- Transition to `validation_failed` or `failed`.
- Pause progression and require operator review.
- Preserve validation summary and table-level failures in run-state.

### CDC degradation/failure

- Mark CDC status as `degraded` or `failed`.
- Block cutover readiness until lag/freshness gates recover.
- Use replication checkpoints for replay planning.

### Cutover failure

- Transition to `rollback_in_progress`.
- Persist operator notes and recovery path details.
- Record rollback trigger reason and post-incident evidence.

## Recovery model

- Keep run-state persisted with schema-versioned payloads.
- Ensure deserialization remains backward compatible via payload migration.
- Use event payload contracts to preserve operational integrations.

## Operator runbook minimums

- explicit pause/resume ownership
- rollback initiation criteria
- failure communication paths (data engineering + SRE + app owners)
- post-cutover validation and sign-off criteria
