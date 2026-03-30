from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from sdk.adapters.contracts import ValidationAdapter


class SamplingStrategy(str, Enum):
    FULL = "full"
    LIMIT = "limit"
    MODULO = "modulo"


@dataclass(frozen=True)
class SamplingConfig:
    strategy: SamplingStrategy = SamplingStrategy.FULL
    limit_rows: int | None = None
    modulo_column: str | None = None
    modulo_base: int | None = None
    modulo_remainder: int = 0


@dataclass(frozen=True)
class ValidationThreshold:
    max_absolute_diff: float = 0.0
    max_percent_diff: float = 0.0


@dataclass(frozen=True)
class AggregateCheck:
    function: str
    column: str


@dataclass(frozen=True)
class TableValidationConfig:
    table_name: str
    compare_row_count: bool = True
    aggregates: list[AggregateCheck] = field(default_factory=list)
    sampling: SamplingConfig = field(default_factory=SamplingConfig)
    threshold: ValidationThreshold = field(default_factory=ValidationThreshold)


@dataclass(frozen=True)
class ValidationCheckResult:
    check_name: str
    passed: bool
    source_value: float
    target_value: float
    absolute_diff: float
    percent_diff: float
    threshold: ValidationThreshold


@dataclass(frozen=True)
class TableValidationResult:
    table_name: str
    checks: list[ValidationCheckResult]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)


@dataclass(frozen=True)
class ValidationReport:
    passed: bool
    table_results: list[TableValidationResult]
    total_checks: int
    failed_checks: int


class ValidationOrchestrator:
    """Runs source/target checks and produces a pass/fail report with diffs."""

    def __init__(self, source_adapter: ValidationAdapter, target_adapter: ValidationAdapter):
        self._source = source_adapter
        self._target = target_adapter

    def run(self, table_configs: list[TableValidationConfig]) -> ValidationReport:
        table_results: list[TableValidationResult] = []
        total_checks = 0
        failed_checks = 0

        for table_config in table_configs:
            checks: list[ValidationCheckResult] = []

            if table_config.compare_row_count:
                result = self._compare_metric(
                    table_name=table_config.table_name,
                    metric_sql="COUNT(*)",
                    check_name="row_count",
                    threshold=table_config.threshold,
                    sampling=table_config.sampling,
                )
                checks.append(result)

            for aggregate in table_config.aggregates:
                metric_sql = f"{aggregate.function.upper()}({aggregate.column})"
                check_name = f"{aggregate.function.lower()}_{aggregate.column}"
                result = self._compare_metric(
                    table_name=table_config.table_name,
                    metric_sql=metric_sql,
                    check_name=check_name,
                    threshold=table_config.threshold,
                    sampling=table_config.sampling,
                )
                checks.append(result)

            total_checks += len(checks)
            failed_checks += len([check for check in checks if not check.passed])
            table_results.append(TableValidationResult(table_name=table_config.table_name, checks=checks))

        return ValidationReport(
            passed=failed_checks == 0,
            table_results=table_results,
            total_checks=total_checks,
            failed_checks=failed_checks,
        )

    def _compare_metric(
        self,
        *,
        table_name: str,
        metric_sql: str,
        check_name: str,
        threshold: ValidationThreshold,
        sampling: SamplingConfig,
    ) -> ValidationCheckResult:
        source_value = self._run_metric_query(self._source, table_name, metric_sql, sampling)
        target_value = self._run_metric_query(self._target, table_name, metric_sql, sampling)

        absolute_diff = abs(source_value - target_value)
        denominator = max(abs(source_value), 1.0)
        percent_diff = (absolute_diff / denominator) * 100.0

        passed = absolute_diff <= threshold.max_absolute_diff and percent_diff <= threshold.max_percent_diff

        return ValidationCheckResult(
            check_name=check_name,
            passed=passed,
            source_value=source_value,
            target_value=target_value,
            absolute_diff=absolute_diff,
            percent_diff=percent_diff,
            threshold=threshold,
        )

    def _run_metric_query(
        self,
        adapter: ValidationAdapter,
        table_name: str,
        metric_sql: str,
        sampling: SamplingConfig,
    ) -> float:
        query = self._build_metric_query(table_name, metric_sql, sampling)
        rows = adapter.execute_query(query)
        if not rows:
            return 0.0

        value = rows[0].get("metric_value")
        if value is None:
            return 0.0
        return float(value)

    def _build_metric_query(self, table_name: str, metric_sql: str, sampling: SamplingConfig) -> str:
        if sampling.strategy == SamplingStrategy.FULL:
            return f"SELECT {metric_sql} AS metric_value FROM {table_name};"

        if sampling.strategy == SamplingStrategy.LIMIT:
            if sampling.limit_rows is None or sampling.limit_rows <= 0:
                raise ValueError("SamplingStrategy.LIMIT requires a positive limit_rows value.")
            return (
                f"SELECT {metric_sql} AS metric_value "
                f"FROM (SELECT * FROM {table_name} LIMIT {sampling.limit_rows}) sample;"
            )

        if sampling.strategy == SamplingStrategy.MODULO:
            if not sampling.modulo_column:
                raise ValueError("SamplingStrategy.MODULO requires modulo_column.")
            if sampling.modulo_base is None or sampling.modulo_base <= 0:
                raise ValueError("SamplingStrategy.MODULO requires a positive modulo_base.")
            return (
                f"SELECT {metric_sql} AS metric_value "
                f"FROM {table_name} "
                f"WHERE ({sampling.modulo_column} % {sampling.modulo_base}) = {sampling.modulo_remainder};"
            )

        raise ValueError(f"Unsupported sampling strategy: {sampling.strategy}")
