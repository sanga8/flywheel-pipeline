import pytest
import pandas as pd
import shutil
import os
import glob
from flywheel.pipeline import DataPipeline
from flywheel.config import PipelineConfig


@pytest.fixture
def clean_output():
    if os.path.exists("test_output"):
        shutil.rmtree("test_output")
    yield "test_output"
    if os.path.exists("test_output"):
        shutil.rmtree("test_output")


def test_config_paths():
    # Verify that the configured paths actually point to files
    for vendor, config in PipelineConfig.VENDORS.items():
        pattern = str(config["file_pattern"])
        files = glob.glob(pattern)
        assert len(files) > 0, f"No files found for {vendor} pattern: {pattern}"


@pytest.mark.parametrize("vendor_key", list(PipelineConfig.VENDORS.keys()))
def test_ingest_vendor(vendor_key):
    pipeline = DataPipeline()
    config = PipelineConfig.VENDORS[vendor_key]
    df = pipeline.ingestion.ingest_vendor(vendor_key, config)

    assert not df.empty, f"Ingestion failed or returned empty for {vendor_key}"

    # Check for mapped columns
    # After ingestion, we expect standardized column names
    expected_cols = ["campaign_id", "timestamp", "impressions", "clicks", "spend"]
    for col in expected_cols:
        assert col in df.columns, f"Missing mapped column '{col}' for {vendor_key}"

    # Check for system columns
    assert "_vendor" in df.columns
    assert "_record_id" in df.columns


def test_end_to_end_run(clean_output):
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
    pipeline = DataPipeline()
    data = {
        "campaign_id": ["c1", None],
        "timestamp": ["2023-01-01", "invalid"],
        "impressions": [100, -1],
        "clicks": [10, 0],
        "spend": [1.0, 1.0],
        "vendor": ["A", "A"],
        "record_id": ["r1", "r2"],
    }
    df = pd.DataFrame(data)
    processed = pipeline.validation.standardize_and_validate(df)

    assert "_event_date" in processed.columns
    assert str(processed.iloc[0]["_event_date"]) == "2023-01-01"
    # NaT timestamp results in explicit UNKNOWN partition
    assert str(processed.iloc[1]["_event_date"]) == "UNKNOWN"

    assert processed.iloc[0]["_is_valid"]
    assert not processed.iloc[1]["_is_valid"]
    assert "invalid_timestamp" in processed.iloc[1]["_dq_issues"]
