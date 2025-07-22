from google.cloud import storage
import pandas as pd
import logging

def upload_df_to_cloud_storage(
    df: pd.DataFrame,
    bucket_name: str,
    object_path: str
) -> None:
    """
    Arguments:
    - df: Pandas dataframe to upload
    - bucket_name: Cloud Storage bucket name
    - object_path: Full object path (e.g. 'tmp/matches/20250722/matches_batch_000001.json')
    - file_extension

    Uploads dataframe to Cloud Storage in NDJSON format.
    """
    try:

        # initialize client
        client = storage.Client()
    
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_path)

        data = df.to_json(orient="records", lines=True)
        blob.upload_from_string(data, content_type="application/json")

        logging.info(f"Uploaded data to gs://{bucket_name}/{object_path}")

    except Exception as e:
        logging.error(f"Error when uploading data to gs://{bucket_name}/{object_path}: {e}.")