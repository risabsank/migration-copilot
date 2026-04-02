from typing import cast

from sdk.adapters.contracts import (
    ColumnInfo,
    MetadataAdapter,
    TableMetadata,
    ValidationAdapter,
)


class FakeMetadataAdapter:
    def list_tables(self, schema: str = "public") -> list[str]:
        return ["users"]

    def describe_table(self, table_name: str, schema: str = "public") -> TableMetadata:
        return TableMetadata(
            table_name=table_name,
            row_estimate=1,
            size_bytes_estimate=1024,
            primary_key_columns=["id"],
            columns=[ColumnInfo(name="id", data_type="int", nullable=False)],
        )


class FakeValidationAdapter:
    def execute_query(self, query: str) -> list[dict]:
        return [{"metric_value": 1}]


def test_metadata_adapter_contract():
    adapter = FakeMetadataAdapter()
    assert isinstance(adapter, MetadataAdapter)
    typed = cast(MetadataAdapter, adapter)
    assert typed.list_tables() == ["users"]


def test_validation_adapter_contract():
    adapter = FakeValidationAdapter()
    assert isinstance(adapter, ValidationAdapter)
    typed = cast(ValidationAdapter, adapter)
    assert typed.execute_query("select 1")[0]["metric_value"] == 1
