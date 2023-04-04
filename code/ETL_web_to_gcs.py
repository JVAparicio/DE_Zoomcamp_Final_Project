import os
#import requests
import pandas as pd
#import pyarrow.parquet as pq

import gitlab
from pathlib import Path
env_path = Path('./..') / '.env'
from dotenv import load_dotenv   #for python-dotenv method
load_dotenv(dotenv_path=env_path) #for python-dotenv method


from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash

#@task(retries=3)
def get_filenames():
    """Get list of filenames from gitlab repository"""
    gitlab_password = os.environ.get('gitlab_password')
    gl = gitlab.Gitlab('https://gitlab.com', private_token=gitlab_password, api_version=4)
    gl.auth()
    project = gl.projects.get('recommend.games/bgg-ranking-historicals')
    items = project.repository_tree(get_all=True)
    file_dict = {file['name'] for file in items}

    return file_dict

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read bgg data from web into pandas Dataframe"""

    df = pd.read_csv(dataset_url, header=0 ,delimiter=(','), names=["ID", "Name", "Year", "Rank", "Average", "Bayes average", "Users rated", "URL", "Thumbnail"], nrows=100)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with missing values"""
    df = df.dropna()
    return df

@task()
def write_local(df: pd.DataFrame, year: str, dataset_file: str) -> Path:
    "Write DataFrame out locally as parquet file"
    output_dir = Path(f"../data/{year}/")
    output_dir.mkdir(parents=True, exist_ok=True)

    path = Path(f"../data/{year}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")

    #path = Path(f"data/{color}")
    #df.to_parquet(path, compression="gzip", partition_cols=["year", "month","day"])
    return path

@task()
def write_gcs(local_path: Path, gcs_path: Path) -> None:
    """Uploading local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")   
    gcs_block.upload_from_path(
        from_path=local_path,
        to_path=gcs_path
    )
    return 

@flow()
def etl_web_to_gcs(files: list, file_date: str, year: str) -> None:
    """The main ETL function"""

    

    df_final = pd.DataFrame()

    print(file_date)
    print(len(files))

    for file in files:

        dataset_url = f"https://gitlab.com/recommend.games/bgg-ranking-historicals/-/raw/master/{file}"
        df = fetch(dataset_url)
        df['date'] = file_date
        df_final = pd.concat([df_final, df], ignore_index=True)
 
    df_clean = clean(df)

    dataset_file = file_date
    local_path = write_local(df_clean, year, dataset_file)
    gcs_path = Path(f"data/{year}/{dataset_file}.parquet")
    write_gcs(local_path, gcs_path)




if __name__ == '__main__':

    # Define the directory to store the downloaded files
    download_dir = "./../data/"

    # Make sure the download directory exists
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    
    file_dict = get_filenames()
    
    # Define the years and months to download the files for
    start_year = 2018
    end_year = 2018

    #Current year
    #end_year = int(pd.Timestamp.now().strftime("%Y"))

    months = list(range(1,13))

    for year in range(start_year, end_year+1):
        for month in range(1,13):
            file_date = f"{year}-{month:02}"

            files_to_download = [file for file in file_dict if file.startswith(file_date)]
            files_to_download.sort()

            etl_web_to_gcs(files_to_download, file_date, year)
