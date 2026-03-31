from __future__ import annotations

import json
from pathlib import Path


def generate_dagster_assets(plan_path: str | Path, output_path: str | Path) -> Path:
    """Host-side adapter example: convert plan steps into Dagster assets."""
    plan_doc = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    steps = plan_doc["result"]["plan"]["steps"]

    lines = ["from dagster import asset", ""]
    for step in steps:
        deps = [d.replace("-", "_") for d in step["depends_on"]]
        dep_expr = f", deps={deps}" if deps else ""
        fn_name = step["id"].replace("-", "_")
        lines.append(f"@asset(name='{fn_name}'{dep_expr})")
        lines.append(f"def {fn_name}():")
        lines.append(f"    return 'stage={step['stage']}: {step['details']}'")
        lines.append("")

    output = Path(output_path)
    output.write_text("\n".join(lines), encoding="utf-8")
    return output
