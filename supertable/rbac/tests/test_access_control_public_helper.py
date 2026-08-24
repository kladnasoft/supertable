import pytest

from supertable.rbac.access_control import check_requested_columns


def test_check_requested_columns_enforces_include_minus_exclude_case_insensitively():
    policy = {
        "columns": ["Order_ID", "Amount", "Internal_Note"],
        "exclude_columns": ["internal_note"],
    }

    check_requested_columns(policy, ["order_id", "AMOUNT"], "orders")

    with pytest.raises(PermissionError, match="internal_note"):
        check_requested_columns(policy, ["INTERNAL_NOTE"], "orders")

    with pytest.raises(PermissionError, match="unknown"):
        check_requested_columns(policy, ["unknown"], "orders")
