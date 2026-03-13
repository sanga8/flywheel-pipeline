import pandas as pd
import json
import logging
from typing import Dict, Any
import glob
import os
import shutil

logger = logging.getLogger("FlywheelPipeline")


class IngestionEngine:
    """Handles data ingestion from various sources."""

    def _read_json(self, file_path: str) -> pd.DataFrame:
        try:
            with open(file_path, "r") as f:
                data = json.load(f)

            # Normalize to get columns
            return pd.json_normalize(data)
        except Exception as e:
            logger.error(f"Error reading JSON {file_path}: {e}")
            return pd.DataFrame()

    def _read_csv(self, file_path: str, config: Dict[str, Any]) -> pd.DataFrame:
        encodings = config.get("csv_encoding_options", ["utf-8"])
        for encoding in encodings:
            try:
                return pd.read_csv(file_path, encoding=encoding)
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error reading CSV {file_path}: {e}")
                return pd.DataFrame()
        logger.error(f"Failed to read CSV {file_path}")
        return pd.DataFrame()

    def write_raw_layer(
        self,
        vendor_name: str,
        config: Dict[str, Any],
        raw_base_path: str,
        ingestion_date: str,
    ) -> int:
        pattern = config.get("file_pattern")
        if not pattern or not isinstance(pattern, str):
            logger.warning(f"No valid file pattern for raw write ({vendor_name})")
            return 0

        files = glob.glob(pattern)
        if not files:
            logger.warning(f"No files found for raw write ({vendor_name})")
            return 0

        vendor_partition = vendor_name.lower().replace(" ", "_")
        destination_dir = os.path.join(
            raw_base_path,
            f"_ingestion_date={ingestion_date}",
            f"_vendor={vendor_partition}",
        )
        os.makedirs(destination_dir, exist_ok=True)

        copied_count = 0
        for source_file in files:
            destination_file = os.path.join(
                destination_dir, os.path.basename(source_file)
            )
            shutil.copy2(source_file, destination_file)
            copied_count += 1

        logger.info(
            f"Raw layer write complete for {vendor_name}: {copied_count} file(s) -> {destination_dir}"
        )
        return copied_count

    def ingest_vendor(
        self,
        vendor_name: str,
        config: Dict[str, Any],
        file_pattern: str | None = None,
    ) -> pd.DataFrame:
        pattern = file_pattern or config.get("file_pattern")
        if not pattern or not isinstance(pattern, str):
            logger.warning(f"No valid file pattern for {vendor_name}")
            return pd.DataFrame()

        files = glob.glob(pattern)
        if not files:
            logger.warning(f"No files found for {vendor_name}")
            return pd.DataFrame()

        dfs = []
        for file_path in files:
            logger.info(f"Ingesting {file_path} for {vendor_name}")

            if config["file_type"] == "json":
                df = self._read_json(file_path)
            elif config["file_type"] == "csv":
                df = self._read_csv(file_path, config)
            else:
                logger.error(f"Unsupported file type: {config['file_type']}")
                continue

            if not df.empty:
                rename_map = {
                    k: v for k, v in config["mapping"].items() if k in df.columns
                }
                df = df.rename(columns=rename_map)
                # Normalize vendor name to be filesystem-safe (e.g., "Vendor A" -> "vendor_a")
                df["_vendor"] = vendor_name.lower().replace(" ", "_")

                # Generate ID based on source columns (campaign + timestamp) + vendor
                # This ensures stability even if new metric columns are added
                id_cols = ["_vendor", "campaign_id", "timestamp"]

                # Check if columns exist before using them for ID generation
                available_id_cols = [c for c in id_cols if c in df.columns]

                hash_input = df[available_id_cols].fillna("").astype(str)
                df["_record_id"] = (
                    pd.util.hash_pandas_object(hash_input, index=False)
                    .astype("uint64")
                    .astype(str)
                )
                dfs.append(df)

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
