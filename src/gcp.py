"""Module to manage file in gcp."""

import json
import logging

from google.cloud import storage
from google.oauth2 import service_account

from src.utils import ROOT_PATH

# Set constants
PROJECT_ID = "beatporter"
BUCKET_NAME = "beatporter"

logger = logging.getLogger("gcp")


# Get service account info
def get_gcp_client_info() -> storage.Client:
    """Get GCP client info from service account."""
    with open(ROOT_PATH + "data/beatporter-sa.json") as source:
        service_account_info = json.load(source)

    storage_credentials = service_account.Credentials.from_service_account_info(
        service_account_info
    )

    storage_client = storage.Client(project=PROJECT_ID, credentials=storage_credentials)

    return storage_client


def get_gcs_blob(file_name: str, bucket_folder: str = "") -> storage.Blob:
    """Get blob from GCS.

    Uses PROJECT_ID and BUCKET_NAME set in the module.

    Args:
        file_name: Local filename
        bucket_folder: Destination folder in the bucket

    Returns:
        Blob object
    """
    bucket = get_gcp_client_info().bucket(BUCKET_NAME)
    destination_blob_name = bucket_folder + file_name
    blob = bucket.blob(destination_blob_name)

    return blob


def download_file_to_gcs(file_name: str, local_folder: str, gcs_folder: str = "") -> None:
    """Download local file from GCS.

    Uses PROJECT_ID and BUCKET_NAME set in the module.

    Args:
        file_name: Local filename
        local_folder: Local source folder
        gcs_folder: Destination folder in the bucket
    """
    blob = get_gcs_blob(file_name=file_name, bucket_folder=gcs_folder)
    blob.download_to_filename(filename=local_folder + file_name)

    logger.info(
        f"Done downloading file {file_name} from blob {BUCKET_NAME + '/' + gcs_folder}"
    )


def upload_file_to_gcs(file_name: str, local_folder: str, gcs_folder: str = "") -> None:
    """Upload local file to GCS.

    Use PROJECT_ID and BUCKET_NAME set in the module.

    Args:
        file_name: Local filename
        local_folder: Local source folder
        gcs_folder: Destination folder in the bucket
    """
    blob = get_gcs_blob(file_name=file_name, bucket_folder=gcs_folder)
    blob.upload_from_filename(filename=local_folder + file_name, if_generation_match=None)

    logger.info(
        f"Done uploading file {file_name} to blob {BUCKET_NAME + '/' + gcs_folder}"
    )
