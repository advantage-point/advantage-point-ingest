from google.cloud import storage
from typing import (
    Dict,
    List
)
import logging

def get_cloud_storage_objects(
    bucket_name: str,
    prefix: str
) -> List[Dict]:
    """
    See docs for more information: https://cloud.google.com/storage/docs/listing-objects#list-objects

    Arguments:
    - bucket_name: Cloud Storage bucket name
    - prefix: Prefix used to filter objects

    List all blobs with file prefix.
    """

    try:

        logging.info(f"Listing objects in bucket ({bucket_name}) with file prefix ({prefix}).")

        # intialize client
        client = storage.Client()

        # get blobs
        blobs = client.list_blobs(
            bucket_name,
            prefix=prefix
        )

        # loop through blobs and return list of dicts
        blob_list = [
            {
                'name': blob.name
            } 
            for blob in blobs
        ]

        return blob_list
    
    except Exception as e:
        logging.error(f"Error listing objects in bucket ({bucket_name}) with file prefix ({prefix}).")
        return []