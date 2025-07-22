from google.cloud import storage
import logging

def delete_cloud_storage_object(
    bucket_name,
    object_name
):
    """
    See docs for more info: https://cloud.google.com/storage/docs/deleting-objects#delete-object

    Arguments:
    - bucket_name: Cloud Storage bucket name
    - object_name: Cloud Storage object name

    Deletes a blob from the bucket.
    """
    
    try:

        # initialize client
        client = storage.Client()

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        generation_match_precondition = None

        # Optional: set a generation-match precondition to avoid potential race conditions
        # and data corruptions. The request to delete is aborted if the object's
        # generation number does not match your precondition.
        blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
        generation_match_precondition = blob.generation

        blob.delete(if_generation_match=generation_match_precondition)

        logging.info(f"Object {object_name} deleted.")

    except Exception as e:
        logging.error(f"Error when deleting object {object_name}: {e}.")