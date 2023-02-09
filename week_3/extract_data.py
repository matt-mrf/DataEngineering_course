import os
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def extract_data(month: int) -> Path:
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month:02}.csv.gz"
    filename = f"fhv_data_2019_{month:02}"
    if url.endswith(".csv.gz"):
        csv_name = f"{filename}.csv.gz"
    else:
        csv_name = f"{filename}.csv"

    os.system(f"wget {url} -O week_3/data/{csv_name}")
    path = Path(f"week_3/data/{csv_name}")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to gcs"""
    gcs_block = GcsBucket.load("zoomcamp-1")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)
    return


@flow()
def etl_web_to_gcs(month: int) -> None:
    path = extract_data(month)
    # write_gcs(path)


@flow()
def etl_parent_flow(month_list: list[int]) -> None:
    for month in month_list:
        etl_web_to_gcs(month)


if __name__ == "__main__":
    month_list = [1]
