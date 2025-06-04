import pandas as pd
import glob
from datetime import datetime
from datetime import date
import subprocess
from AZ_BlobStorage import upload_all_csv_files
from AZ_BlobStorage import get_data_from_azure_blob

import json

#readme
#make sure config in ./net-read-excel\appsettings.json is set to the correct paths

with open('config.json', 'r') as f:
    config = json.load(f)

connection_string = config['connection_string']
container_url = config['container_url']
local_folder = config['local_folder']

# Get str files from Azure Blob Storage
print("Downloading STR files from Azure Blob Storage...")
get_data_from_azure_blob(local_folder, container_url, connection_string)

#run processor that convert xlsx to csv
print("Running the Excel to CSV conversion process...")
exe_path = './net-read-excel/STR-ReadExcel.exe'
result = subprocess.run([exe_path, '1'], check=True)
print(f"Process exited with code: {result.returncode}")


#upload to azure storage
print("Uploading CSV files to Azure Blob Storage...")
upload_all_csv_files(local_folder, container_url, connection_string)