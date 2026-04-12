# Local Execution Guide

This repository is designed to let teams generate migration plans and persisted run-state safely in local environments before promoting workflows to CI or production.

## Prerequisites

- Python 3.10+
- `pip`
- Optional: a source database instance when validating real adapters

## Install

```bash
python -m pip install -e .[dev]
```

## Run core quality checks locally

```bash
ruff check .
mypy --explicit-package-bases sdk/state sdk/adapters sdk/observability.py tests/test_contracts_hardening.py tests/test_output_snapshots.py
pytest -q
```

## Generate a plan bundle

```bash
python -m sdk.cli plan --spec examples/spec.json --out ./artifacts/local-demo
```

Expected artifacts include:

- `plan.json`
- `runbook.md`
- `validations.sql`
- `events.jsonl`
- `governance/checksums.json`

## Validate persisted run-state schema compatibility

Run the contract tests for persisted run-state and events:

```bash
pytest -q tests/test_contracts_hardening.py
```

These tests verify:

- run-state payload schema/version fields
- legacy payload migration behavior
- event payload stability
- runtime protocol checks for adapters
