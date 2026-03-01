import pandas as pd
import os
import logging
import shutil
from flywheel.config import PipelineConfig
from flywheel.core.ingestion import IngestionEngine
from flywheel.core.validation import ValidationEngine

logger = logging.getLogger("FlywheelPipeline")


class DataPipeline:
    def __init__(self, output_dir="output"):
        self.output_dir = output_dir
        self.ingestion = IngestionEngine()
        self.validation = ValidationEngine()

    def run(self):
        all_dfs = []

        # Iterate over configured vendors
        for vendor_name, config in PipelineConfig.VENDORS.items():
            logger.info(f"Starting ingestion for {vendor_name}...")
            df = self.ingestion.ingest_vendor(vendor_name, config)
            if not df.empty:
                all_dfs.append(df)

        if not all_dfs:
            logger.warning("No data ingested from any vendor.")
            return

        full_df = pd.concat(all_dfs, ignore_index=True)
        processed_df = self.validation.standardize_and_validate(full_df)
        output_path = os.path.join(self.output_dir, "marketing_performance")

        try:
            # Clean up existing partitions to ensure idempotency (simple overwrite strategy)
            # In a real S3 scenario, this would involve deleting objects under the partition prefix.
            if os.path.exists(output_path):
                logger.info(f"Cleaning up existing output at {output_path}...")
                shutil.rmtree(output_path)

            os.makedirs(output_path, exist_ok=True)

            processed_df.to_parquet(
                output_path,
                partition_cols=["_event_date", "_vendor"],
                engine="pyarrow",
                index=False,
                compression="snappy",
            )
            logger.info(f"Result written to {output_path}")

            summary = processed_df.groupby(["_vendor", "_is_valid"]).size()
            logger.info("\nProcessing Summary:\n" + str(summary))

            # Check for invalid records and warn
            invalid_count = int(processed_df[~processed_df["_is_valid"]].shape[0])
            if invalid_count > 0:
                logger.warning(
                    f"Pipeline finished with {invalid_count} INVALID records. Check data quality."
                )

            return {
                "status": "success",
                "output_path": output_path,
                "metrics": {
                    "total_rows": int(len(processed_df)),
                    "valid_rows": int(processed_df["_is_valid"].sum()),
                    "invalid_rows": invalid_count,
                    "vendors_processed": list(processed_df["_vendor"].unique()),
                },
            }

        except Exception as e:
            logger.error(f"Failed to write output: {e}")


if __name__ == "__main__":
    # Setup Logging when running as a script to avoid overriding
    # logging configuration when imported as a module
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    DataPipeline().run()
