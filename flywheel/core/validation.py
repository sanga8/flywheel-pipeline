import pandas as pd
from datetime import datetime
from flywheel.config import PipelineConfig


class ValidationEngine:
    """Handles data standardization and validation."""

    @staticmethod
    def _add_issue(row, message):
        current = row["_dq_issues"]
        return f"{current}; {message}" if current else message

    def standardize_and_validate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        current_time = datetime.now()

        # Ensure columns exist
        for col in PipelineConfig.REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # Numeric coercion
        for col in PipelineConfig.METRIC_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Timestamp parsing
        df["_event_ts"] = pd.to_datetime(
            df["timestamp"], errors="coerce", utc=True, format="mixed"
        )

        # Data Quality Checks
        df["_is_valid"] = True
        df["_dq_issues"] = ""

        # Invalid Timestamps
        mask_bad_ts = df["_event_ts"].isna()
        if mask_bad_ts.any():
            df.loc[mask_bad_ts, "_is_valid"] = False
            df.loc[mask_bad_ts, "_dq_issues"] = df.loc[mask_bad_ts].apply(
                lambda x: self._add_issue(x, "invalid_timestamp"), axis=1
            )

        # Negative Metrics
        mask_neg = pd.Series([False] * len(df), index=df.index)
        for col in PipelineConfig.METRIC_COLUMNS:
            mask_neg |= df[col] < 0

        if mask_neg.any():
            df.loc[mask_neg, "_is_valid"] = False
            df.loc[mask_neg, "_dq_issues"] = df.loc[mask_neg].apply(
                lambda x: self._add_issue(x, "negative_metrics"), axis=1
            )

        # Mandatory fields
        for field in PipelineConfig.MANDATORY_FIELDS:
            if field in df.columns:
                mask_missing = df[field].isna() | (
                    df[field].astype(str).str.strip() == ""
                )
                if mask_missing.any():
                    df.loc[mask_missing, "_is_valid"] = False
                    df.loc[mask_missing, "_dq_issues"] = df.loc[mask_missing].apply(
                        lambda x: self._add_issue(x, f"missing_{field}"), axis=1
                    )

        # Deduplication
        is_dup = df.duplicated(subset=["_vendor", "_record_id"], keep="first")
        if is_dup.any():
            df.loc[is_dup, "_is_valid"] = False
            df.loc[is_dup, "_dq_issues"] = df.loc[is_dup].apply(
                lambda x: self._add_issue(x, "duplicate_record"), axis=1
            )

        # Populate system columns
        df["_ingestion_at"] = current_time

        # Derive date part for efficient partitioning by event time
        df["_event_date"] = df["_event_ts"].dt.strftime("%Y-%m-%d").fillna("UNKNOWN")

        # We want all source columns defined in config, excluding the raw 'timestamp'
        # since we have 'event_timestamp' now
        source_cols = [c for c in PipelineConfig.REQUIRED_COLUMNS if c != "timestamp"]

        # Reorder to put technical columns at the end
        final_cols = (
            source_cols + PipelineConfig.INTERNAL_COLUMNS + PipelineConfig.DEBUG_COLUMNS
        )

        for c in final_cols:
            if c not in df.columns:
                df[c] = None

        return df[final_cols]
