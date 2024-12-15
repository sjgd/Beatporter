"""Module to manage file in gcp."""
import json
import logging

from google.cloud import storage
from google.oauth2 import service_account

# Set constants
PROJECT_ID = "beatporter"
BUCKET_NAME = "beatporter"

logger = logging.getLogger("gcp")


# Get service account info
def get_gcp_client_info():
    """Get GCP client info from service account."""
    with open("../data/beatporter-sa.json") as source:
        service_account_info = json.load(source)

    storage_credentials = service_account.Credentials.from_service_account_info(
        service_account_info
    )

    storage_client = storage.Client(project=PROJECT_ID, credentials=storage_credentials)

    return storage_client


def upload_file_to_gcs(file_name: str, source_folder: str, destination_folder=""):
    """Upload local file to GCS.

    Use PROJECT_ID and BUCKET_NAME set in the module.

    Args:
        file_name: Local filename
        source_folder: Local source folder
        destination_folder: Destination folder in the bucket

    """
    bucket = get_gcp_client_info().bucket(BUCKET_NAME)
    destination_blob_name = destination_folder + file_name
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_folder + file_name, if_generation_match=None)

    logger.info(
        f"Done uploading file {file_name} "
        f"to blob {BUCKET_NAME+'/' +destination_folder}"
    )
