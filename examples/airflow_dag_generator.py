from __future__ import annotations

import json
from pathlib import Path


def generate_airflow_dag(plan_path: str | Path, output_path: str | Path) -> Path:
    """Host-side adapter example: turn generated plan.json into an Airflow DAG file."""
    plan_doc = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    steps = plan_doc["result"]["plan"]["steps"]

    lines = [
        "from airflow import DAG",
        "from airflow.operators.empty import EmptyOperator",
        "from datetime import datetime",
        "",
        "with DAG('migration_copilot_plan', start_date=datetime(2025, 1, 1), schedule=None, catchup=False) as dag:",
    ]

    for step in steps:
        task_id = step["id"].replace("-", "_")
        lines.append(f"    {task_id} = EmptyOperator(task_id='{task_id}')")

    for step in steps:
        for dep in step["depends_on"]:
            dep_id = dep.replace("-", "_")
            task_id = step["id"].replace("-", "_")
            lines.append(f"    {dep_id} >> {task_id}")

    output = Path(output_path)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output
