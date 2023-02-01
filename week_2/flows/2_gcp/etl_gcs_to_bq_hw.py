import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta
from prefect_gcp import GcpCredentials


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_from_gcs(color: str, year: int, month: int) -> pd.DataFrame:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoomcamp-1")
    gcs_block.get_directory(from_path=gcs_path, local_path=("../gcs_dl/"))
    path = Path(f"../gcs_dl/{gcs_path}")
    return pd.read_parquet(path)


# @task()
# def transform(path: Path) -> pd.DataFrame:
#     """Data cleaning example"""
#     df = pd.read_parquet(path)
#     print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
#     df["passenger_count"].fillna(0, inplace=True)
#     print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")

#     return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_cred_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table="de_zoomcamp.2019_data",
        project_id="dtc-dataengineering-375516",
        credentials=gcp_cred_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> pd.DataFrame:
    """Main ETL flow to load data into BigQuery"""

    df = extract_from_gcs(color, year, month)
    write_bq(df)
    return df


@flow(log_prints=True)
def etl_bq_parent_flow(months: list[int], year: int, color: str):
    record_count = 0
    for month in months:
        df = etl_gcs_to_bq(year, month, color)
        record_count += len(df)

    print("Processing finished... \n")
    print(f"Total amount of rows processed: {record_count}.")


if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_bq_parent_flow(months, year, color)
