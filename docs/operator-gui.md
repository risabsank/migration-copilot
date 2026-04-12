# Operator GUI (Migration Control Plane)

The repository now includes a lightweight operator-facing web UI backed by stable service APIs.

## What it provides

- Run list and run detail views.
- Lifecycle status and phase progression.
- Per-table backfill execution progress.
- Validation status and summaries.
- CDC/catch-up lag and readiness visibility.
- Cutover readiness and explicit blocking conditions.
- Rollback readiness and rollback plan state.
- Timeline/audit log and approval history.
- AI recommendation summary (advisory with policy profile shown).
- Incident pack viewer endpoint.
- Monitoring dashboard cards for health/SLO, completion, and lag.

## Local run

1. Install dependencies:

   ```bash
   pip install -e .
   ```

2. Start the API/UI server (from repo root):

   ```bash
   python -m sdk.control_plane.api
   ```

3. Open:

   - UI: `http://127.0.0.1:8000/`

## API to backend mapping

- Persistence source of truth: `JsonMigrationRunStore` (existing run state).
- Orchestration controls: `MigrationOrchestrator`.
- Rollback trigger flow: policy-guarded lifecycle transitions persisted to run state.
- Policy/approval checks: `ExecutionPolicyEngine`.
- Health and incident summaries: `RunHealthMonitoringService`.

The frontend only calls API routes and does not contain migration business logic.

## Operator actions

Mutating API actions all execute policy evaluation before high-risk operations:

- Start/resume/pause orchestration.
- Retry failed table.
- Request approval.
- Approve/deny gated action.
- Trigger rollback.

For rollback and other gated operations, denied policy decisions are returned as HTTP 403.
