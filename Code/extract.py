import json
import logging
import requests
from google.cloud import storage

def fetch_data(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {api_url}: {e}")
        return None

def save_json(data, filename):
    if data:
        with open(filename, "w") as f:
            json.dump(data, f)
        logging.info(f"Data saved to {filename}")
    else:
        logging.warning(f"No data to save in {filename}")

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        logging.info(f"Uploaded {source_file_name} to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        logging.error(f"Failed to upload {source_file_name} to GCS: {e}")

def fetch_and_store_data(api_endpoints, bucket_name):
    for key, url in api_endpoints.items():
        data = fetch_data(url)
        local_filename = f"{key}_data.json"
        save_json(data, local_filename)
        upload_to_gcs(bucket_name, local_filename, f"raw/{local_filename}")
