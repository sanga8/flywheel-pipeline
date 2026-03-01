# Design Document

## Data Model

### Unified Schema
We are targeting a centralized data lake approach where disparate vendor data is normalized into a single `analytics.marketing_performance` table/schema.

**Target Schema:**

*Columns added by the ingestion starts with `_`. We could make it `flywheel_` to clearly differenciate from the source columns if needed.*

- `campaign_id` (String): Unique identifier for the campaign.
- `timestamp` (String): Original timestamp from vendor.
- `_event_ts` (Timestamp): Parsed event time in UTC.
- `_vendor` (String): 'Vendor_A' or 'Vendor_B'.
- `impressions` (Integer): Count of ad impressions.
- `clicks` (Integer): Count of ad clicks.
- `spend` (Float): Cost associated with the record.
- `_record_id` (String): Deterministic hash of `_vendor`, `campaign_id`, and `timestamp` for deduplication.
- `_event_date` (Date): Partition key, derived from the source event timestamp (UTC).
- `_ingestion_at` (Timestamp): Timestamp when the data was processed on our side.
- `_is_valid` (Boolean): Data quality flag (True if all validations pass).
- `_dq_issues` (String): List of data quality issues if any.
- `_raw_data` (String): Original JSON dump of the record for recoverability.

## Partitioning Strategy
**Strategy:** Hive-style partitioning by `_event_date` (event date) and `_vendor`.
**Path structure:** `s3://analytics-data/marketing_performance/_event_date=YYYY-MM-DD/_vendor=VENDOR_NAME/part-001.parquet`

**Cost & speed optimized for analytics:** Queries typically filter based on when the event occurred (e.g., "campaign spend last week").
**Backfilling:**  If we find an error in the data for Jan 15th, we  only need to re-process and overwrite the folder _event_date=2024-01-15.