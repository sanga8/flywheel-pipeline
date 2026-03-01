import sys
import os

# Add project root to python path to allow importing flywheel
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pendulum
from airflow.sdk import dag, task
from flywheel.pipeline import DataPipeline


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["vendors"],
)
def flywheel_vendors_pipeline():

    @task
    def ingest_vendors():
        return DataPipeline().run()

    ingest_vendors()


flywheel_vendors_pipeline()
