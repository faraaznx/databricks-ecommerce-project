import pytest
from pyspark.sql import Row
from common_util import normalize_columns

def test_normalize_columns_success(spark):
    df = spark.createDataFrame(
        [Row(**{"User Name": "A", "Order-ID": 1})]
    )

    result = normalize_columns(df)

    assert result.columns == ["user_name", "order_id"]
