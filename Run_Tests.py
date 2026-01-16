# Databricks notebook source
from utils.common_util import normalize_columns

# COMMAND ----------

import pytest

# COMMAND ----------

# MAGIC %pytest tests/test_common_util.py  -p no:cacheprovider
# MAGIC
# MAGIC

# COMMAND ----------

import os
import pytest

os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"

pytest.main(["tests", "-p", "no:cacheprovider", "--assert=plain"])


# COMMAND ----------

import os
import pytest

# Absolutely required in Databricks Repos
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"

pytest.main([
    "tests/test_common_util.py",
    "-p", "no:cacheprovider",
    "--assert=plain"
])

