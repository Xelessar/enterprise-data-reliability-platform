import pandas as pd

from etl.transform import drop_exact_duplicates, normalize_columns, strip_string_columns


def test_normalize_columns_lowercases_and_strips_spaces():
    df = pd.DataFrame(columns=[" Customer ID ", "Order Value"])
    result = normalize_columns(df)
    assert list(result.columns) == ["customer_id", "order_value"]


def test_strip_string_columns_removes_whitespace():
    df = pd.DataFrame({"name": ["  Alice ", "Bob  "], "value": [1, 2]})
    result = strip_string_columns(df)
    assert result["name"].tolist() == ["Alice", "Bob"]


def test_drop_exact_duplicates_removes_full_row_dupes():
    df = pd.DataFrame({"id": [1, 1, 2], "value": [10, 10, 20]})
    result = drop_exact_duplicates(df)
    assert len(result) == 2
