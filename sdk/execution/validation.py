"""Executable validation plumbing for migration run gating."""

from __future__ import annotations

import re
from dataclasses import dataclass

from sdk.adapters.contracts import ValidationAdapter
from sdk.observability import EventCollector
from sdk.state.models import (
    MigrationRun,
    MigrationRunStatus,
    ValidationCheck,
    ValidationCheckStatus,
    ValidationResult,
    ValidationStatus,
    ValidationSummary,
)
from sdk.state.store import MigrationRunStore


@dataclass(frozen=True)
class ValidationGate:
    """Simple deterministic gate for validation progression."""

    max_failed_checks: int = 0
    allow_unknown: bool = False


@dataclass(frozen=True)
class ValidationCheckSpec:
    """Intermediate executable check shape."""

    check_name: str
    table_name: str | None
    query: str
    threshold: float = 0.0


class ValidationExecutor:
    """Execute SQL validation checks and persist migration run outcomes."""

    def __init__(
        self,
        *,
        adapter: ValidationAdapter,
        store: MigrationRunStore,
        collector: EventCollector | None = None,
        gate: ValidationGate | None = None,
    ):
        self._adapter = adapter
        self._store = store
        self._collector = collector
        self._gate = gate or ValidationGate()

    def execute(self, *, run: MigrationRun, validations_sql: str) -> MigrationRun:
        if run.status == MigrationRunStatus.FAILED:
            raise RuntimeError("Cannot execute validations for a failed migration run")

        if run.status == MigrationRunStatus.BACKFILLING:
            run.transition_to(MigrationRunStatus.VALIDATING)
        elif run.status not in {MigrationRunStatus.VALIDATING, MigrationRunStatus.VALIDATION_PASSED, MigrationRunStatus.VALIDATION_FAILED}:
            raise RuntimeError(f"Run {run.run_id} must be in validating phase before executing validations")

        run.validation_status = ValidationStatus.IN_PROGRESS
        self._store.save(run)
        self._emit("validation_execution_started", "started", run, {"run_id": run.run_id})

        check_specs = self._parse_sql_checks(validations_sql)
        checks_by_table: dict[str, list[ValidationCheck]] = {}

        for spec in check_specs:
            self._emit(
                "validation_check_started",
                "running",
                run,
                {"run_id": run.run_id, "check_name": spec.check_name, "table_name": spec.table_name},
            )
            check = self._execute_check(spec)
            table_key = check.table_name or "__migration__"
            checks_by_table.setdefault(table_key, []).append(check)
            self._emit(
                "validation_check_completed",
                check.status.value,
                run,
                {
                    "run_id": run.run_id,
                    "check_name": check.check_name,
                    "table_name": check.table_name,
                    "difference": check.difference,
                    "threshold": check.threshold,
                },
            )

        table_results = [
            ValidationResult(table_name=table_name, checks=checks)
            for table_name, checks in sorted(checks_by_table.items(), key=lambda item: item[0])
        ]
        summary = self._build_summary(table_results)
        run.validation_summary = summary

        gate_passed = summary.failed_checks <= self._gate.max_failed_checks and (
            self._gate.allow_unknown or summary.unknown_checks == 0
        )

        if gate_passed:
            run.validation_status = ValidationStatus.PASSED
            run.transition_to(MigrationRunStatus.VALIDATION_PASSED)
        else:
            run.validation_status = ValidationStatus.FAILED
            run.transition_to(MigrationRunStatus.VALIDATION_FAILED)

        self._store.save(run)
        self._emit(
            "validation_execution_completed",
            run.validation_status.value,
            run,
            {
                "run_id": run.run_id,
                "status": run.validation_status.value,
                "failed_checks": summary.failed_checks,
                "unknown_checks": summary.unknown_checks,
                "total_checks": summary.total_checks,
            },
        )
        return run

    def _execute_check(self, spec: ValidationCheckSpec) -> ValidationCheck:
        rows = self._adapter.execute_query(spec.query)
        if not rows:
            return ValidationCheck(
                check_name=spec.check_name,
                table_name=spec.table_name,
                query=spec.query,
                status=ValidationCheckStatus.UNKNOWN,
                threshold=spec.threshold,
                details="Validation query returned no rows",
            )

        row = rows[0]
        source_value = self._coerce_number(row, "source_count", "source_pk_checksum", "source_value")
        target_value = self._coerce_number(row, "target_count", "target_pk_checksum", "target_value")

        if source_value is None or target_value is None:
            return ValidationCheck(
                check_name=spec.check_name,
                table_name=spec.table_name,
                query=spec.query,
                status=ValidationCheckStatus.UNKNOWN,
                threshold=spec.threshold,
                details="Validation query did not contain source/target comparable values",
            )

        difference = abs(source_value - target_value)
        status = ValidationCheckStatus.PASSED if difference <= spec.threshold else ValidationCheckStatus.FAILED
        return ValidationCheck(
            check_name=spec.check_name,
            table_name=spec.table_name,
            query=spec.query,
            status=status,
            source_value=source_value,
            target_value=target_value,
            difference=difference,
            threshold=spec.threshold,
        )

    def _build_summary(self, table_results: list[ValidationResult]) -> ValidationSummary:
        checks = [check for table in table_results for check in table.checks]
        passed_checks = len([item for item in checks if item.status == ValidationCheckStatus.PASSED])
        failed_checks = len([item for item in checks if item.status == ValidationCheckStatus.FAILED])
        unknown_checks = len([item for item in checks if item.status == ValidationCheckStatus.UNKNOWN])

        if failed_checks > 0:
            status = ValidationCheckStatus.FAILED
        elif unknown_checks > 0:
            status = ValidationCheckStatus.UNKNOWN
        else:
            status = ValidationCheckStatus.PASSED

        return ValidationSummary(
            status=status,
            total_checks=len(checks),
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            unknown_checks=unknown_checks,
            table_results=table_results,
        )

    def _parse_sql_checks(self, validations_sql: str) -> list[ValidationCheckSpec]:
        non_comment_lines = [
            line
            for line in validations_sql.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        normalized_sql = "\n".join(non_comment_lines)
        statements = [item.strip() for item in normalized_sql.split(";") if item.strip()]
        checks: list[ValidationCheckSpec] = []

        for index, statement in enumerate(statements, start=1):
            table_name = self._extract_table_name(statement)
            metric_name = "checksum" if "checksum" in statement.lower() else "row_count"
            check_name = f"{table_name or 'migration'}_{metric_name}_{index}"
            checks.append(
                ValidationCheckSpec(
                    check_name=check_name,
                    table_name=table_name,
                    query=f"{statement};",
                )
            )

        return checks

    def _extract_table_name(self, statement: str) -> str | None:
        match = re.search(r"SELECT\s+'([^']+)'\s+AS\s+table_name", statement, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def _coerce_number(self, row: dict[str, object], *keys: str) -> float | None:
        for key in keys:
            if key not in row:
                continue
            value = row[key]
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
        return None

    def _emit(self, event_type: str, status: str, run: MigrationRun, payload: dict[str, object]) -> None:
        if not self._collector:
            return
        self._collector.emit(
            event_type=event_type,
            step="validation_executor",
            status=status,
            payload=payload,
        )
