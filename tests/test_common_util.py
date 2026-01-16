"""
Tests for utils.common_util

Scope:
- normalize_columns
- convert_date_columns

"""

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType

from utils.common_util import (
    normalize_columns,
    convert_date_columns
)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    # This retrieves the active Spark session already running in Databricks
    return SparkSession.builder.getOrCreate()


# ============================================================
# normalize_columns tests
# ============================================================

def test_normalize_columns_success(spark):
    """
    Column names should be normalized to:
    - lowercase
    - underscores instead of spaces or hyphens
    """
    df = spark.createDataFrame(
        [(1, "Alice")],
        ["User ID", "User-Name"]
    )

    result_df = normalize_columns(df)

    assert isinstance(result_df, DataFrame)
    assert result_df.columns == ["user_id", "user_name"]


def test_normalize_columns_preserves_data(spark):
    """
    Column normalization must not mutate row-level data.
    """
    df = spark.createDataFrame(
        [(1, "Alice"), (2, "Bob")],
        ["User ID", "User Name"]
    )

    result_df = normalize_columns(df)

    assert result_df.collect() == df.collect()


def test_normalize_columns_none_input():
    """
    Defensive check: None input should fail fast with a clear error.
    """
    with pytest.raises(ValueError, match="Input DataFrame is None"):
        normalize_columns(None)


def test_normalize_columns_wrong_type():
    """
    Defensive check: non-DataFrame inputs are not allowed.
    """
    with pytest.raises(TypeError, match="Input must be a PySpark DataFrame"):
        normalize_columns("not_a_dataframe")


def test_normalize_columns_empty_schema(spark):
    """
    A DataFrame without columns is considered invalid input.
    """
    df = spark.createDataFrame([(1,)], ["id"])
    df = df.select()  # explicitly drop all columns

    with pytest.raises(ValueError, match="Input DataFrame has no columns"):
        normalize_columns(df)


# ============================================================
# convert_date_columns tests
# ============================================================

def test_convert_date_columns_success_single_column(spark):
    """
    Valid date strings should be converted to DateType
    using the provided format.
    """
    df = spark.createDataFrame(
        [("2024-01-10",)],
        ["event_date"]
    )

    result_df = convert_date_columns(
        df=df,
        columns=["event_date"],
        date_format="yyyy-MM-dd"
    )

    assert isinstance(result_df.schema["event_date"].dataType, DateType)


def test_convert_date_columns_success_multiple_columns(spark):
    """
    Multiple columns should be converted in a single call.
    """
    df = spark.createDataFrame(
        [("2024-01-10", "2024-01-12")],
        ["start_date", "end_date"]
    )

    result_df = convert_date_columns(
        df=df,
        columns=["start_date", "end_date"],
        date_format="yyyy-MM-dd"
    )

    assert isinstance(result_df.schema["start_date"].dataType, DateType)
    assert isinstance(result_df.schema["end_date"].dataType, DateType)


def test_convert_date_columns_invalid_date_results_in_null(spark):
    """
    Spark behavior:
    - Invalid date strings are converted to NULL
    - No exception is raised
    """
    df = spark.createDataFrame(
        [("invalid-date",)],
        ["event_date"]
    )

    result_df = convert_date_columns(
        df=df,
        columns=["event_date"],
        date_format="yyyy-MM-dd"
    )

    assert result_df.collect()[0]["event_date"] is None


def test_convert_date_columns_preserves_row_count(spark):
    """
    Date conversion must not alter row cardinality.
    """
    df = spark.createDataFrame(
        [("2024-01-10",), ("2024-01-11",)],
        ["event_date"]
    )

    result_df = convert_date_columns(
        df=df,
        columns=["event_date"],
        date_format="yyyy-MM-dd"
    )

    assert result_df.count() == df.count()


def test_convert_date_columns_df_is_none():
    """
    Defensive check: None DataFrame should raise immediately.
    """
    with pytest.raises(ValueError, match="Input DataFrame is None"):
        convert_date_columns(None, ["date"], "yyyy-MM-dd")


def test_convert_date_columns_wrong_df_type():
    """
    Defensive check: df must be a PySpark DataFrame.
    """
    with pytest.raises(TypeError, match="Input must be a PySpark DataFrame"):
        convert_date_columns("not_a_dataframe", ["date"], "yyyy-MM-dd")


def test_convert_date_columns_columns_not_list(spark):
    """
    Columns argument must be a list to avoid ambiguous behavior.
    """
    df = spark.createDataFrame(
        [("2024-01-10",)],
        ["event_date"]
    )

    with pytest.raises(TypeError, match="Columns must be a list"):
        convert_date_columns(df, "event_date", "yyyy-MM-dd")


def test_convert_date_columns_columns_empty(spark):
    """
    An empty columns list is treated as invalid input.
    """
    df = spark.createDataFrame(
        [("2024-01-10",)],
        ["event_date"]
    )

    with pytest.raises(ValueError, match="Columns list cannot be empty"):
        convert_date_columns(df, [], "yyyy-MM-dd")


def test_convert_date_columns_date_format_missing(spark):
    """
    Date format is mandatory for deterministic parsing.
    """
    df = spark.createDataFrame(
        [("2024-01-10",)],
        ["event_date"]
    )

    with pytest.raises(ValueError, match="Date format must be provided"):
        convert_date_columns(df, ["event_date"], None)


def test_convert_date_columns_missing_column(spark):
    """
    Attempting to convert a non-existent column should fail explicitly.
    """
    df = spark.createDataFrame(
        [("2024-01-10",)],
        ["event_date"]
    )

    with pytest.raises(ValueError, match="does not exist in DataFrame"):
        convert_date_columns(df, ["missing_date"], "yyyy-MM-dd")
