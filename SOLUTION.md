# Solution Overview

### Architecture Principle
The curated `marketing_performance` table should contain standardized business fields and technical audit metadata only.
Raw payload retention belongs to a dedicated landing/bronze dataset.

## Meeting Business Requirements

1.  **Unified Analytics across Vendors:**
    *   **Challenge:** Analysts need to query Vendor A (JSON) and Vendor B (CSV) or any vendor together.
    *   **Solution:** The pipeline normalizes disparate schemas into a single internal schema. Field mappings (e.g., `metrics.impressions` -> `impressions`) are handled in `PipelineConfig`. The output is a single dataset where the `vendor` column distinguishes the source, allowing aggregations like `SUM(clicks) GROUP BY vendor`.

2.  **Reliable Trends & Timestamps:**
    *   **Challenge:** Vendor B has inconsistent date formats (`MM-DD-YYYY` vs `YYYY-MM-DD`).
    *   **Solution:** The `ValidationEngine` explicitly parses dates with `errors='coerce'` and standardizes them to UTC. Invalid dates are not dropped but flagged, preserving the original `timestamp` string for debugging while providing a clean `_event_ts` for trend analysis.

3.  **Data Quality & Freshness:**
    *   **Challenge:** Analysts need to know which records are valid and when data arrived.
    *   **Solution:**
        *   **Quality:** Implemented a `_is_valid` boolean flag and a `_dq_issues` column. Analysts can filter `WHERE is_valid = True` for clean reporting or analyze `dq_issues` to report back to vendors.
        *   **Freshness:** The pipeline adds an `_ingestion_date` partition key (YYYY-MM-DD) representing the processing time, separate from the source event time. This allows tracking data arrival.

4.  **Cost Visibility:**
    *   **Challenge:** Leadership needs spend per vendor/campaign.
    *   **Solution:** `spend`, `campaign_id`, and `vendor` are mandatory columns in the standardized schema. Numeric coercion ensures `spend` is a valid float, treating non-numeric values as 0.0 (or null) to prevent aggregation errors.

5.  **Schema Evolution & Recoverability:**
    *   **Challenge:** Vendors often change data formats (e.g. adding new metrics) without notice.
    *   **Solution:** Kept the ingestion model focused on normalized analytics fields and metadata only (`_record_id`, `_vendor`, `_event_ts`, `_event_date`, `_is_valid`, `_dq_issues`).
    *   **Trade-off:** This keeps the table lean and query-friendly, but source-level replay/debug should rely on landing/source files rather than an in-row raw payload.

## Implementation Choices

### Configuration
To satisfy the requirement of easily integrating more vendors, I implemented a configuration-driven architecture.
- **`PipelineConfig` Class**: Centralizes all vendor-specific logic (file patterns, formats, column mappings).
- **Generic Ingestion**: The `ingest_vendor` method handles different file types (JSON, CSV) based on configuration, rather than hardcoded methods per vendor. Adding a "Vendor C" only requires adding a dictionary entry to `PipelineConfig.VENDORS`, without modifying the core pipeline logic.

### Language & Framework
**Python & Pandas**:
- Chosen for ease of implementation, readability.
- **Trade-off:** While Pandas is great for single-node processing, it is memory-bound. For 10 - 200 GB I would switch to Polar or DuckDB. For TB-scale data, I would switch to PySpark to handle distributed processing. The current design uses modular transformation functions that can easily be ported to Spark UDFs or map transformations. I would go with dlt for the ingestion and dbt for the transformation.

### File Format
**Parquet**:
- **Compression:** Columnar storage offers superior compression compared to CSV/JSON.
- **Schema Enforcement:** Embeds schema information, preventing type drift.
- **Performance:** Much faster for read-heavy analytics workloads.

### Data Quality
Implemented a "Flag, Don't Fail" strategy for row-level issues:

Invalid rows are not dropped silently. They are flagged with `_is_valid=False` and a `_dq_issues` column is added for explanation.

This allows analysts to quantify data quality issues and decide if they want to exclude them or attempt recovery, rather than losing data visibility entirely.

### Filesystem Abstraction (S3 vs Local)
For this assessment, I used the local filesystem (`glob.glob`) to simplify testing and development. S3 URIs (`s3://...`) instead of local paths.

## Future Improvements
- **Transformation:** After the ingestion dag finishes, we could run a dbt project to aggregate the data, remove the unused technical columns and create fact and dimension tables etc. for analytics usage.
- **Alerting:** Integrate with Slack/PagerDuty/GoogleChat/Email.. on the airflow dag to notify on high failure rates (e.g., if >10% of rows are invalid).
- **Testing:** The project is quite small but if it grows we can separate unit and integration tests in different folders and also have the CI pipeline validate the airflow dag parsing.
- **Idempotency & Re-runs:** In a production distributed system, filesystem deletes are not atomic. I would upgrade to **Apache Iceberg**, which handles atomic `INSERT OVERWRITE` operations to guarantee no dirty reads even during mid-write failures.
- **Configuration Management:** The `config.py` can be replaced with **Pydantic models** loaded from a YAML/TOML file. This would provide strict validation of the configuration itself (e.g., ensuring `file_pattern` is a valid string, `mapping` is complete) before the pipeline even starts.
- **Apache Iceberg:** While I used standard Parquet files for this assessment (to keep it lightweight and portable), implementing Iceberg on top would be the logical next step for a production data lake.
    - It provides ACID transactions, schema evolution (handling vendor changes gracefully), and time travel for debugging data issues.
- **DLT (Data Load Tool):** To further reduce boilerplate code, the custom ingestion logic could be replaced with **dlt**. It automatically contracts and evolves the schema when vendors add new columns, which solves the "maintenance burden" of manual mapping.
- **Analytics with AI:** On top of dashboards, we could add [nao](https://github.com/getnao/nao) - which I'm a recent contributor of - to use natural language to query our data.