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
def write_bq(df: pd.DataFrame, table_name: str) -> None:
    """Writer DataFrame to Big Query"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-credentials")

    df.to_gbq(
        destination_table=table_name,
        project_id=gcp_credentials_block.project,
        credentials= gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append"
    )

@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int):
    """Main ETL flow to load data into Big Query"""
    
    path = extract_from_gcs(year, month)
    df = transform(path)
    table_name = f"ds_zoomcamp.top100_{year}"
    write_bq(df, table_name)


if __name__ == '__main__':
    year = 2022    
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    for month in months:
        etl_gcs_to_bq(year, month)
    