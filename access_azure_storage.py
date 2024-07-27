from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()

client_id = os.environ['AZURE_CLIENT_ID']
tenant_id = os.environ['AZURE_TENANT_ID']
secret = os.environ['AZURE_CLIENT_SECRET']
storage_url = os.environ['AZURE_STORAGE_URL']

credentials = ClientSecretCredential(tenant_id, client_id, secret)


def getBlobData(): 
    container_name = "storageblobcontainerpup"
    file_name = "sample.txt"

    blob_service_client = BlobServiceClient(storage_url, credentials)
    container = blob_service_client.get_container_client(container=container_name)
    blob_client = container.get_blob_client(blob=file_name)

    data = blob_client.download_blob().readall().decode("utf-8")

    return data

def listBlobs():
    container_name = "storageblobcontainerpup"
    
    blob_service_client = BlobServiceClient(storage_url, credentials)
    container_client = blob_service_client.get_container_client(container=container_name)

    blob_iterator = container_client.list_blob_names()

    return ('\n').join(blob_iterator)


def uploadBlob():
    container_name = "storageblobcontainerpup"
    local_dir = "upload"
    blob_service_client = BlobServiceClient(storage_url, credentials)
    container_client = blob_service_client.get_container_client(container=container_name)

    files = os.listdir(local_dir)

    for file in files:
        file_path = os.path.join(local_dir, file)

        with open(file_path, "r") as f1:
            data = f1.read()
            container_client.upload_blob(name=file, data=data)


if __name__ == "__main__":
    print(getBlobData())
    print(listBlobs())
    uploadBlob()



