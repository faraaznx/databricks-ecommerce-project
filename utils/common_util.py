from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, col
from typing import List

def normalize_columns(df: DataFrame) -> DataFrame:
    """
    Normalize DataFrame column names:
    - lowercase
    - replace spaces and hyphens with underscores

    Raises:
        ValueError: if df is None or has no columns
        TypeError: if input is not a DataFrame
    """
    if df is None:
        raise ValueError("Input DataFrame is None")

    if not isinstance(df, DataFrame):
        raise TypeError("Input must be a PySpark DataFrame")

    if not df.columns:
        raise ValueError("Input DataFrame has no columns")

    normalized_columns = [
        c.lower().replace(" ", "_").replace("-", "_")
        for c in df.columns
    ]

    return df.toDF(*normalized_columns)

def convert_date_columns(
    df: DataFrame,
    columns: List[str],
    date_format: str
) -> DataFrame:
    """
    Convert specified columns to DateType using given format.

    Raises:
        ValueError: if df or inputs are invalid
        TypeError: if columns is not a list
    """
    if df is None:
        raise ValueError("Input DataFrame is None")

    if not isinstance(df, DataFrame):
        raise TypeError("Input must be a PySpark DataFrame")

    if not columns:
        raise ValueError("Columns list cannot be empty")

    if not isinstance(columns, list):
        raise TypeError("Columns must be a list")

    if not date_format:
        raise ValueError("Date format must be provided")

    df_columns = set(df.columns)

    for column in columns:
        if column not in df_columns:
            raise ValueError(f"Column '{column}' does not exist in DataFrame")

        df = df.withColumn(column, to_date(col(column), date_format))

    return df
