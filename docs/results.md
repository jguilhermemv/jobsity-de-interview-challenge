# Results

## Bronze To Silver Retry Fix
- Root cause: `src/de_challenge/spark/job1.py` created PySpark `Column` expressions at module import time, which raised `AssertionError` before the Spark job started under Airflow.
- Fix: moved those predicates into helper functions evaluated only during batch processing.
- Regression coverage: added `tests/spark/test_job1_import.py` to verify the module imports without an active Spark context.

## Validation
- Run `pytest tests/spark/test_job1_import.py`.
- Re-run the Airflow DAG and confirm `bronze_to_silver_stream` remains `running` instead of returning to `up_for_retry`.

## Bronze To Silver Silver Projection Fix
- Root cause: `src/de_challenge/spark/job1.py` dropped `value` and `timestamp` from the Silver projection before using them to build `raw_payload` and `kafka_timestamp`.
- Fix: extracted `_build_silver_trips_df()` so metadata columns are derived first and only then projected to the Silver schema.
- Regression coverage: added `tests/spark/test_job1_silver_projection.py` to verify the Silver projection keeps `raw_payload` and `kafka_timestamp` without leaking `value` or `timestamp`.

## Validation
- Run `pytest tests/spark/test_job1_import.py tests/spark/test_job1_silver_projection.py`.
- Clear and rerun `bronze_to_silver_stream` in Airflow, then confirm it no longer fails with `UNRESOLVED_COLUMN` for `value`.

## Runtime Rebuild Documentation Fix
- Root cause: the running `api` and `airflow` containers were built from older images, so Docker was executing stale code even though the workspace files were already fixed.
- Fix: rebuilt the affected services and documented in `README.md` that this project requires `docker compose up -d --build` after code or Dockerfile changes affecting those containers.
- Validation: confirmed the live `api` container was missing the `ingestion_end` logic before rebuild, then confirmed the rebuilt container loaded the updated `_publish_events()` implementation.
