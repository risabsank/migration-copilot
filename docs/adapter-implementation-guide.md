# Adapter Implementation Guide

Adapters are the integration boundary between your metadata/validation systems and the migration control plane.

## Required protocols

Implement these contracts from `sdk.adapters.contracts`:

- `MetadataAdapter`
  - `list_tables(schema: str = "public") -> list[str]`
  - `describe_table(table_name: str, schema: str = "public") -> TableMetadata`
- `ValidationAdapter`
  - `execute_query(query: str) -> list[dict[str, Any]]`

## Implementation expectations

- Return deterministic ordering from `list_tables`.
- Ensure `describe_table` returns stable, normalized estimates and PK/FK info.
- Use safe defaults for missing metadata (never return malformed records).
- Keep adapter logic side-effect-free (metadata read, no DDL/DML).

## Contract testing

Use contract tests as a release gate:

```bash
pytest -q tests/test_contracts_hardening.py tests/test_adapater_contracts.py
```

Focus on:

- runtime protocol conformance (`isinstance(..., MetadataAdapter)`) 
- valid return shapes for metadata/validation records
- negative tests for incomplete implementations

## Version compatibility guidance

When updating adapter behavior that changes table ordering, FK discovery, or size estimates:

1. re-run snapshot tests
2. review generated runbook/plan diffs
3. document compatibility impact for downstream orchestrators
