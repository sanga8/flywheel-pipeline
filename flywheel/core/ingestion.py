import pandas as pd
import json
import logging
import hashlib
from typing import Dict, Any
import glob

logger = logging.getLogger("FlywheelPipeline")


class IngestionEngine:
    """Handles data ingestion from various sources."""

    def generate_record_id(self, row_values):
        """Generates a stable hash for a row to identify uniqueness."""
        row_str = str(tuple(row_values))
        return hashlib.md5(row_str.encode("utf-8")).hexdigest()

    def _read_json(self, file_path: str) -> pd.DataFrame:
        try:
            with open(file_path, "r") as f:
                data = json.load(f)

            # Normalize to get columns
            df = pd.json_normalize(data)

            # Store raw JSON for schema evolution/auditing
            # We convert the list of dicts back to strings row by row
            if isinstance(data, list):
                _raw_data = [json.dumps(record) for record in data]
                df["_raw_data"] = _raw_data

            return df
        except Exception as e:
            logger.error(f"Error reading JSON {file_path}: {e}")
            return pd.DataFrame()

    def _read_csv(self, file_path: str, config: Dict[str, Any]) -> pd.DataFrame:
        encodings = config.get("csv_encoding_options", ["utf-8"])
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding)

                # For CSV, we can store the row as a JSON string to be consistent
                # orient='records' converts each row to a dict
                # Note: this is expensive for large CSVs, but acceptable for this scale
                df["_raw_data"] = df.apply(lambda row: row.to_json(), axis=1)

                return df
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error reading CSV {file_path}: {e}")
                return pd.DataFrame()
        logger.error(f"Failed to read CSV {file_path}")
        return pd.DataFrame()

    def ingest_vendor(self, vendor_name: str, config: Dict[str, Any]) -> pd.DataFrame:
        pattern = config.get("file_pattern")
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

                df["_record_id"] = df[available_id_cols].apply(
                    lambda x: self.generate_record_id(x.values), axis=1
                )
                dfs.append(df)

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
