class PipelineConfig:
    """
    Configuration for vendor data sources.
    Add more vendors or update file patterns as needed.
    """

    VENDORS = {
        "Vendor_A": {
            "file_pattern": "sample_data/vendor-a*.json",
            "file_type": "json",
            "mapping": {
                "campaign_id": "campaign_id",
                "vendor_timestamp": "timestamp",
                "metrics.impressions": "impressions",
                "metrics.clicks": "clicks",
                "metrics.spend": "spend",
            },
        },
        "Vendor_B": {
            "file_pattern": "sample_data/vendor-b*.csv",
            "file_type": "csv",
            "mapping": {
                "ad_network_id": "campaign_id",
                "report_date": "timestamp",
                "impressions": "impressions",
                "clicks": "clicks",
                "spend_usd": "spend",
            },
            "csv_encoding_options": ["utf-8", "latin1", "cp1252"],
        },
    }

    # Target schema for validation
    REQUIRED_COLUMNS = ["campaign_id", "timestamp", "impressions", "clicks", "spend"]

    # Numeric metrics to be coerced and validated
    METRIC_COLUMNS = ["impressions", "clicks", "spend"]

    # System columns added by the pipeline
    # '_event_date' added for partitioning by event time
    INTERNAL_COLUMNS = [
        "_record_id",
        "_vendor",
        "_ingestion_at",
        "_event_ts",
        "_event_date",
    ]
    DEBUG_COLUMNS = ["_is_valid", "_dq_issues"]

    # Fields that must not be null or empty string
    MANDATORY_FIELDS = ["campaign_id", "_vendor", "timestamp"]
