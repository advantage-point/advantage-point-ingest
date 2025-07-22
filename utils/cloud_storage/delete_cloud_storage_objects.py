from utils.cloud_storage.delete_cloud_storage_object import delete_cloud_storage_object
from utils.cloud_storage.get_cloud_storage_objects import get_cloud_storage_objects
import logging

def delete_cloud_storage_objects(
    bucket_name: str,
    prefix: str
):
    """
    Arguments:
    - bucket_name: Cloud Storage bucket name
    - prefix: Prefix used to filter objects

    Deletes all objects with file prefix.
    """

    try:
        
        logging.info(f"Deleting objects in bucket ({bucket_name}) with prefix ({prefix}).")

        object_list = get_cloud_storage_objects(
            bucket_name=bucket_name,
            prefix=prefix
        )

        for object_dict in object_list:
            
            object_name = object_dict['name']
            delete_cloud_storage_object(
                bucket_name=bucket_name,
                object_name=object_name
            )

    except Exception as e:
        logging.error(f"Error deleting objects in bucket ({bucket_name}) with prefix ({prefix}).")