import importlib
import sys


def test_job1_imports_without_active_spark_context() -> None:
    sys.modules.pop("de_challenge.spark.job1", None)
    module = importlib.import_module("de_challenge.spark.job1")

    assert callable(module.run_job1)
