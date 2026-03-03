import pytest
import pandas as pd
import shutil
import os
import glob
from flywheel.pipeline import DataPipeline
from flywheel.config import PipelineConfig


@pytest.fixture
def clean_output():
    """
    Setup/Teardown fixture to ensure a fresh test directory.

    Removes the 'test_output' directory before the test runs and cleans
    it up after the test finishes to prevent cross-test data pollution.
    """
    if os.path.exists("test_output"):
        shutil.rmtree("test_output")
    yield "test_output"
    if os.path.exists("test_output"):
        shutil.rmtree("test_output")


def test_config_paths():
    """
    Verify that the configuration file patterns actually match existing files.

    This acts as a 'Sanity Check' for the local environment. If this fails,
    the pipeline can't run because the source data is missing.
    """
    for vendor, config in PipelineConfig.VENDORS.items():
        pattern = str(config["file_pattern"])
        files = glob.glob(pattern)
        assert len(files) > 0, f"No files found for {vendor} pattern: {pattern}"


@pytest.mark.parametrize("vendor_key", list(PipelineConfig.VENDORS.keys()))
def test_ingest_vendor(vendor_key):
    """
    Validate individual vendor ingestion and column mapping.

    For each configured vendor, verify that:
    - The data is successfully loaded into a DataFrame.
    - Raw columns are correctly mapped to our standardized internal schema.
    - Mandatory system audit columns (_vendor, _record_id) are attached.
    """
    pipeline = DataPipeline()
    config = PipelineConfig.VENDORS[vendor_key]
    df = pipeline.ingestion.ingest_vendor(vendor_key, config)

    assert not df.empty, f"Ingestion failed or returned empty for {vendor_key}"

    # Verify standard schema exists
    expected_cols = ["campaign_id", "timestamp", "impressions", "clicks", "spend"]
    for col in expected_cols:
        assert col in df.columns, f"Missing mapped column '{col}' for {vendor_key}"

    assert "_vendor" in df.columns
    assert "_record_id" in df.columns


def test_end_to_end_run(clean_output):
    """
    Execute a full pipeline run and verify output persistence.

    Triggers the entire DataPipeline.run() method and checks if:
    - The output directory is created.
    - The processed data is written as Parquet files.
    - The directory structure follows the expected naming convention.
    """
    pipeline = DataPipeline(output_dir=clean_output)
    pipeline.run()

    output_path = os.path.join(clean_output, "marketing_performance")
    assert os.path.exists(output_path)

    # Check for parquet files recursively
    parquet_files = glob.glob(
        os.path.join(output_path, "**", "*.parquet"), recursive=True
    )
    assert len(parquet_files) > 0


def test_validation_logic():
    """
    Verify the 'ValidationEngine' handles both clean and corrupt data.

    Test cases include:
    - Valid Record: Correct date parsing and _is_valid flag set to True.
    - Invalid Timestamp: Handling of bad timestamps, defaulting to
      'UNKNOWN' partition and flagging as invalid.
    - Data Quality (DQ) Flags: Ensures specific issues (like negative
      impressions) are captured in the _dq_issues audit column.
    """
    pipeline = DataPipeline()
    data = {
        "campaign_id": ["c1", None],
        "timestamp": ["2023-01-01", "invalid"],
        "impressions": [100, -1],
        "clicks": [10, 0],
        "spend": [1.0, 1.0],
        "_vendor": ["A", "A"],
        "_record_id": ["rec_1", "rec_2"],
    }
    df = pd.DataFrame(data)
    processed = pipeline.validation.standardize_and_validate(df)

    # Verify Date Standardization
    assert "_event_date" in processed.columns
    assert str(processed.iloc[0]["_event_date"]) == "2023-01-01"

    # Verify Failure Handling
    assert str(processed.iloc[1]["_event_date"]) == "UNKNOWN"
    assert processed.iloc[0]["_is_valid"]
    assert not processed.iloc[1]["_is_valid"]
    assert "invalid_timestamp" in processed.iloc[1]["_dq_issues"]
