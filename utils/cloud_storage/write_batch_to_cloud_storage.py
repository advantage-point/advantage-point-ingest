from google.cloud import storage
from typing import (
    Dict,
    List,
)
import json
import logging

def write_batch_to_cloud_storage(
    record_list: List[Dict],
    bucket_name: str,
    object_path: str
):
    """
    Arguments:
    - record_list: List (of dictionaries) of records to write.
    - bucket_name: Cloud Storage bucket name
    - object_path

    Write record list to Cloud Storage.
    """

    try:
    
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_path)

        # NDJSON string
        lines = [json.dumps(record) for record in record_list]
        blob.upload_from_string('\n'.join(lines), content_type='application/json')

        logging.info(f"Wrote data to bucket {bucket_name} object path {object_path}.")

    except Exception as e:
        logging.error(f"Error when writing to bucket {bucket_name} object path {object_path}: {e}.")
        raise
