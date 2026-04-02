from sdk.engine.models import TableProfile
from sdk.engine.validation import schema_contract_diff


def test_schema_contract_diff_scoring():
    src = [TableProfile(name="users", row_count=1, size_gb=1, has_primary_key=True, column_names=["id", "email"])]
    tgt = [TableProfile(name="users", row_count=1, size_gb=1, has_primary_key=True, column_names=["id"])]
    report = schema_contract_diff(src, tgt)
    assert report.breaking_changes
    assert report.backward_compatibility_score < 1.0
