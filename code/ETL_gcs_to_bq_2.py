from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials



@task(retries=3)
def extract_from_gcs(year: int, month: int) -> Path:
    """Download trip from GCS"""
    gcs_path=f"data/{year}/{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f".")
    return Path(f"{gcs_path}")

@task
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    #print(f"pre:missing passenger count: {df['passenger_count'].isna().sum()}")
    #df["passenger_count"] = df["passenger_count"].fillna(0)
    #print(f"pos:missing passenger count: {df['passenger_count'].isna().sum()}")

    return df


@task
def write_bq(df: pd.DataFrame) -> None:
    """Writer DataFrame to Big Query"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-credentials")

    df.to_gbq(
        destination_table="ds_zoomcamp.test",
        project_id="dtc-de-course-375312",
        credentials= gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append"
    )

@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int):
    """Main ETL flow to load data into Big Query"""
    
    path = extract_from_gcs(year, month)
    df = transform(path)
    write_bq(df)


if __name__ == '__main__':
    year = 2018    
    months = [1,2]
    for month in months:
        etl_gcs_to_bq(year, month)
    