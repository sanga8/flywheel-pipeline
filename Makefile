.PHONY: all lint format type-check test clean airflow clean-airflow

# Run all checks (formatter, linter, type checker, tests)
all: format lint type-check test

format:
	uv run ruff format .

lint:
	uv run ruff check . --fix

type-check:
	uv run ty check

test:
	uv run python -m pytest

clean:
	rm -rf .pytest_cache .ruff_cache __pycache__
	rm -rf */__pycache__
	rm -rf output/
	rm -rf *logs/

airflow:
    # Load .env if it exists, set necessary env vars for Airflow (resolve MAC OS issues), and run in standalone mode
	if [ -f .env ]; then set -a; source .env; set +a; fi && \
	export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES && \
	export NO_PROXY=* && \
	uv run airflow standalone

clean-airflow:
	rm -rf airflow_local/airflow.db*
	rm -rf airflow_local/logs
	rm -rf airflow_local/*.generated
	rm -rf airflow_local/*.pid