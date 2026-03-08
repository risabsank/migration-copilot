from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from sdk.adapters.postgres_metadata import PostgresMetadataAdapter
from sdk.copilot import MigrationCopilot
from sdk.engine.models import MigrationSpec, PolicyProfile


def _connect_postgres(database_url: str):
    try:
        import psycopg  # type: ignore

        return psycopg.connect(database_url)
    except ImportError:
        import psycopg2  # type: ignore

        return psycopg2.connect(database_url)


def main() -> int:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("Set DATABASE_URL to a local Postgres connection string.")
        return 1

    with _connect_postgres(database_url) as connection:
        adapter = PostgresMetadataAdapter(connection)
        copilot = MigrationCopilot(metadata_adapter=adapter)

        spec = MigrationSpec(
            source_type="postgres",
            target_type=os.getenv("TARGET_TYPE", "snowflake"),
            objects=[o.strip() for o in os.getenv("TABLES", "").split(",") if o.strip()],
            downtime_minutes=int(os.getenv("DOWNTIME_MINUTES", "5")),
            policy_profile=PolicyProfile.CONSERVATIVE,
        )

        output = copilot.plan(spec=spec, schema=os.getenv("SOURCE_SCHEMA", "public"), cdc_supported=True)

    print("=== RESOLVED PLAN JSON ===")
    print(json.dumps(output.result.as_dict(), indent=2))
    print("\n=== RUNBOOK ===")
    print(output.runbook_markdown)

    runbook_path = Path("runbook.md")
    runbook_path.write_text(output.runbook_markdown, encoding="utf-8")
    print(f"\nWrote runbook to {runbook_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
