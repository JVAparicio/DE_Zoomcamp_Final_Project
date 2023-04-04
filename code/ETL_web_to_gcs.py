import os
#import requests
import pandas as pd
#import pyarrow.parquet as pq

import gitlab
from pathlib import Path
env_path = Path('./..') / '.env'
from dotenv import load_dotenv   #for python-dotenv method
load_dotenv(dotenv_path=env_path) #for python-dotenv method

# Define the base URL and the file format
#base_url = "https://gitlab.com/recommend.games/bgg-ranking-historicals/-/raw/master/"
#file_format = ".csv"



def fetch(dataset_url: str) -> pd.DataFrame:
    """Read bgg data from web into pandas Dataframe"""

    print(dataset_url)
    df = pd.read_csv(dataset_url, header=0 ,delimiter=(','), names=["ID", "Name", "Year", "Rank", "Average", "Bayes average", "Users rated", "URL", "Thumbnail"], nrows=100)
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with missing values"""
    df = df.dropna()
    return df



def etl_web_to_gcs(file: str) -> None:
    """The main ETL function"""

    #dataset_file = f"{year}-{month:02}*.csv"
    #dataset_file = "2016-10-12T00-30-40.csv"

    dataset_url = f"https://gitlab.com/recommend.games/bgg-ranking-historicals/-/raw/master/{file}"
    df = fetch(dataset_url)

    print(df)
    df_clean = clean(df)
    #path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


if __name__ == '__main__':

    # Define the directory to store the downloaded files
    download_dir = "./../data/"

    # Make sure the download directory exists
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
 
    gitlab_password = os.environ.get('gitlab_password')
    gl = gitlab.Gitlab('https://gitlab.com', private_token=gitlab_password, api_version=4)
    gl.auth()
    project = gl.projects.get('recommend.games/bgg-ranking-historicals')
    items = project.repository_tree(get_all=True)
    file_dict = {file['name'] for file in items}

    # Define the years and months to download the files for
    start_year = 2018
    #Current year
    end_year = int(pd.Timestamp.now().strftime("%Y"))
    months = list(range(1,13))

    for year in range(start_year, end_year+1):
        for month in range(1,13):
            file_date = str(year) +'-'+ str(month)

        files_to_download = [file for file in file_dict if file.startswith(file_date)]

        print(files_to_download)
    #for file in files_to_download:
    #    etl_web_to_gcs(file)


# Define the years to download the files for
#start_year = 2018
#end_year = int(pd.Timestamp.now().strftime("%Y"))

# Loop over the years and download the corresponding files
#for year in range(start_year, end_year+1):
#    # Construct the URL for the file
#    file_url = base_url + str(year) + file_format
#    
#    # Download the file
#    print("Downloading file from URL: ", file_url)
#    response = requests.get(file_url)
#    
#    # Save the file locally
#    file_path = os.path.join(download_dir, str(year) + file_format)
#    with open(file_path, "wb") as f:
#        f.write(response.content)
#        
#    # Load the file into a pandas dataframe
#    df = pd.read_csv(file_path, header=None, names=["ID", "Name", "Year", "Rank", "AverageBayes", "averageUsers", "rated", "URL", "Thumbnail"])
#    
#    # Write the dataframe to Parquet format
#    parquet_path = os.path.join(download_dir, str(year) + ".parquet")
#    pq.write_table(pa.Table.from_pandas(df), parquet_path)
#
#print("All files downloaded and written to Parquet format!")