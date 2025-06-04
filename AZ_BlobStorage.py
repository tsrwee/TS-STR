# ...existing code...
from azure.storage.blob import BlobServiceClient
import os
import shutil

def upload_all_csv_files(local_folder, container_url, connection_string):
    """
    Uploads all CSV files from local_folder to the Azure Blob Storage container,
    placing them in the /data/CSVied directory in the blob.
    After upload, moves the file to ./data/CSVied/ locally, overwriting if exists.
    """
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = container_url.split('/')[-1]
    local_csvied_dir = os.path.join(local_folder, "CSVied")
    os.makedirs(local_csvied_dir, exist_ok=True)

    for root, _, files in os.walk(local_folder):
        # Skip the CSVied directory itself to avoid recursion
        if os.path.abspath(root) == os.path.abspath(local_csvied_dir):
            continue
        for file in files:
            if file.lower().endswith('.csv'):
                file_path = os.path.join(root, file)
                blob_path = f"TourPlan/STR/CSV/{file}"
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
                with open(file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                print(f"Uploaded: {blob_path}")

                # Move the file locally to ./data/CSVied/, overwrite if exists
                dest_path = os.path.join(local_csvied_dir, file)
                shutil.move(file_path, dest_path)
                print(f"Moved locally to: {dest_path}")

def get_data_from_azure_blob(local_folder,container_url, connection_string):
    """
    Downloads all str files from the Azure Blob Storage container to the local folder.
    """
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = container_url.split('/')[-1]
    container_client = blob_service_client.get_container_client(container_name)

    local_folder = os.path.join(local_folder, "Raw/")    
    
    os.makedirs(local_folder, exist_ok=True)  # Ensure the folder exists

    for blob in container_client.list_blobs(name_starts_with='TourPlan/STR/'):
        # Ensure the file is directly under TourPlan/STR/ (not in subfolders)
        relative_path = blob.name[len('TourPlan/STR/'):]
        if '/' in relative_path:
            continue  # Skip files in subfolders
        filename = os.path.basename(blob.name)
        if filename.startswith('TourismSaskatchewan_') and filename.endswith('.xls'):
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob.name)
            file_path = os.path.join(local_folder, filename)
            with open(file_path, "wb") as file:
                data = blob_client.download_blob()
                file.write(data.readall())
            print(f"Downloaded: {file_path}")
            
            # Move the file in Azure Blob Storage from TourPlan/STR/ to TourPlan/STR/Archived/
            source_blob = f"TourPlan/STR/{filename}"
            dest_blob = f"TourPlan/STR/Archived/{filename}"
            source_blob_client = blob_service_client.get_blob_client(container=container_name, blob=source_blob)
            dest_blob_client = blob_service_client.get_blob_client(container=container_name, blob=dest_blob)
            # Start copy
            copy_source_url = source_blob_client.url
            dest_blob_client.start_copy_from_url(copy_source_url)
            # Delete original after copy
            source_blob_client.delete_blob()
            print(f"Moved in Azure from {source_blob} to {dest_blob}")