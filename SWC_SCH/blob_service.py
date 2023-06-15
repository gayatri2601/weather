from azure.storage.blob import BlobServiceClient, BlobClient

def upload_csv_to_blob(storage_connection_string, container_name, file_path, blob_name):
    # Create a BlobServiceClient object using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)

    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)

    # Create a blob client with the desired blob name
    blob_client = container_client.get_blob_client(blob_name)

    # Upload the CSV file to the blob
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data)


