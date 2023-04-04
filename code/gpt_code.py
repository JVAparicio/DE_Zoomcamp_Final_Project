import os
import re
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Define the base URL for the files
base_url = "https://gitlab.com/recommend.games/bgg-ranking-historicals/-/raw/master/"

# Define the directory to store the downloaded files
download_dir = "./downloaded_files/"

# Make sure the download directory exists
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

# Define the range of years to download files for
start_year = 2018
end_year = 2018

# Define the regular expression pattern to match the file name
file_name_pattern = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.csv")

# Loop through each year, month, and day, and download the corresponding file
for year in range(start_year, end_year+1):
    for month in range(1, 13):
        for day in range(1, 32):
            file_name_prefix = f"{year:04d}-{month:02d}-{day:02d}T"
            file_name_suffix = ".csv"
            file_url = base_url + file_name_prefix + "*" + file_name_suffix

            print("file_url:", file_url)
            
            # Find the file name that matches the pattern
            response = requests.get(file_url)
            print(response)
            file_name = None
            for line in response.iter_lines():
                if file_name_pattern.match(line.decode("utf-8")):
                    file_name = line.decode("utf-8")
                    break
            if file_name is None:
                continue
            
            # Download the file
            print(f"Downloading file from URL: {file_url}")
            response = requests.get(base_url + file_name)

            # Save the file locally
            file_path = os.path.join(download_dir, file_name)
            with open(file_path, "wb") as f:
                f.write(response.content)

            # Load the file into a pandas dataframe
            df = pd.read_csv(file_path, header=None, names=["ID", "Name", "Year", "Rank", "AverageBayes", "averageUsers", "rated", "URL", "Thumbnail"])

            # Write the dataframe to Parquet format
            parquet_path = os.path.join(download_dir, os.path.splitext(file_name)[0] + ".parquet")
            pq.write_table(pa.Table.from_pandas(df), parquet_path)

            print(f"File downloaded and written to Parquet format: {parquet_path}")
