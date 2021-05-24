"""
Simple script to download all paisagenslidar data from previously created download table
"""
import pandas as pd
import tqdm

from src.constants import PAISAGENSLIDAR_PATH
from src.utils.download import download

# Get latest version of download table
download_table_path = sorted(list(PAISAGENSLIDAR_PATH.glob("*.csv")))[-1]
download_table = pd.read_csv(download_table_path, index_col=0)

# Select only lidar data for download
lidar_data = download_table[download_table["type"] == "lidar"]

# Download all data
progress_bar = tqdm.tqdm(lidar_data.iterrows(), total=len(lidar_data))
for idx, row in progress_bar:

    url = row.link
    file_path = PAISAGENSLIDAR_PATH / row.folder_structure
    progress_bar.set_description(f"Downloading {url}")

    download(url=url, file_path=file_path, overwrite=False)
