import asyncio
import os
import re

from datetime import timedelta

from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config/dask-raster-worker.json'

# Initialize the Google Cloud Storage client
client = storage.Client()


def get_max_rpms_year_gcs(bucket_name, prefix="rpms/"):
    """
    Finds the maximum year from all 'rpms/YEAR/rpms_YEAR.tif' files in the bucket.
    
    :param bucket_name: Name of the GCS bucket.
    :param prefix: Prefix to filter the blobs (default is 'rpms/').
    :return: The maximum year available in the rpms_YEAR.tif files.
    """
    bucket = client.bucket(bucket_name)
    
    # List all blobs matching the pattern rpms/YEAR/rpms_YEAR.tif
    blobs = bucket.list_blobs(prefix=prefix)
    
    # Use glob-style matching instead of regex
    year_pattern = "rpms/*/rpms_*.tif"
    
    years = []
    for blob in blobs:
        if blob.name.startswith('rpms/') and blob.name.endswith('.tif'):
            # Extract the year from the path (assuming 'rpms/YEAR/rpms_YEAR.tif')
            parts = blob.name.split('/')
            if len(parts) >= 3 and parts[1].isdigit():
                year = int(parts[1])
                years.append(year)

    if years:
        return max(years)  # Return the maximum year
    else:
        return None  # If no matching files are found




def list_blobs_matching_pattern(bucket_name, prefix=""):
    """
    Lists all blobs in the bucket that match the pattern 'rpms_YEAR.tif'.
    
    :param bucket_name: Name of the GCS bucket.
    :param prefix: Prefix to filter the blobs.
    :return: A list of matching blob names.
    """
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    
    # Regex pattern for matching 'rpms_YEAR.tif'
    pattern = re.compile(r"rpms_(\d{4})\.tif")

    matching_blobs = [blob.name for blob in blobs if pattern.match(blob.name)]
    
    return matching_blobs


def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    """
    Uploads a file to GCS.
    
    :param bucket_name: Name of the GCS bucket.
    :param source_file_path: Local path to the file.
    :param destination_blob_name: GCS destination path (including prefix).
    :return: None
    """
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    # Upload the file to GCS
    blob.upload_from_filename(source_file_path)





def generate_signed_url(bucket_name, blob_name, expiration_days=7):
    """
    Generates a signed URL for the given blob that expires in 1 week.
    
    :param bucket_name: Name of the GCS bucket.
    :param blob_name: Name of the blob to generate a signed URL for.
    :param expiration_days: How many days the URL should be valid (default is 7 days).
    :return: A signed URL for the blob.
    """
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Set expiration to 1 week (7 days) by default
    expiration_time = timedelta(days=expiration_days)
    
    # Generate the signed URL
    signed_url = blob.generate_signed_url(expiration=expiration_time)
    
    return signed_url