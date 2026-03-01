# AI Prompts & Usage Log

This document logs the main AI (Github Copilot) interactions used during the development of this pipeline.

## Prompts

- "Create a project structure for a data pipeline take-home assignment including sample_data, tests, and documentation files."
- "Generate realistic sample data for two vendors. Vendor A is JSON with nested objects and duplicates. Vendor B is CSV with date format inconsistencies and encoding issues."
- "Write a Python class to ingest, standardize, and validate the data using Pandas. It needs to handle schema mapping and write to partitioned parquet."
- "Refactor the pipeline to be configuration-driven. Allow adding new vendors by simply updating a configuration dictionary mapping definition (file pattern, type, column mapping)."
- "Add a Makefile to this project to simplify standard developer tasks like formatting, linting, type checking, and running tests. Use `uv` for dependency management."
- "Simplify the package structure. Empty `__init__.py` and use explicit imports (e.g., `from pipeline.main import ...`) instead of re-exporting symbols via `__init__.py`."
- "Rename `main.py` to `pipeline.py` and move logging configuration inside `if __name__ == '__main__':`."
- "Rename the package directory from `pipeline` to `flywheel` to avoid namespace conflicts."
- "Create a GitHub Actions CI workflow (`.github/workflows/ci.yml`) to verify formatting, linting, and tests."
- "Update `SOLUTION.md` to explain the design choice of using local filesystem abstraction for S3 paths."
- "There is a `make test` failure due to missing columns/NaN dates. Fix `validation.py` by adding `UNKNOWN` sentinel for dates and ensuring `_vendor` and `_record_id` exist."
- "Why not same tests for vendor A and B? Refactor `test_pipeline.py` to use `@parametrize` and config-based assertions."
- "Simplify the DAG code in `dags/dag_vendors.py`."
- "Fix `ModuleNotFoundError` in DAG by adding `sys.path` injection."
- "The notebook loads 0 rows because `pyarrow.dataset` fails to discover hive partitions locally. Rewrite `data_presentation.ipynb` using `glob` + `pandas` manual loading."
- "What if we used dlt? Update `SOLUTION.md` to discuss it as an alternative."
- "Run a full code analysis review and provide feedback on strengths and weaknesses."
- "Fix linting error: `E712 Avoid equality comparisons to True` in `data_presentation.ipynb`."
- "Update `DESIGN.md` to reflect the new schema: `_record_id` is now a deterministic hash of business keys, and add `_ingestion_timestamp`."

*An anectode on AI while doing this project: it kept presenting solutions with "1. / 2. / 3. .." (not sure if every model does that). It makes us think there is an order for the tasks whereas most of the time there is not. Using numerical value to present information should only be used when necessary in my opinion.*