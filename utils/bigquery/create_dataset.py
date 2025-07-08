from google.cloud import bigquery
import logging

def create_dataset(
    project_id: str,
    dataset_id: str,
    dataset_location: str
):
    """
    Arguments:
    - project_id: Google Cloud Project ID
    - dataset_id: Dataset name
    - dataset_location: Dataset geographic location

    Create BigQuery dataset if it does not exist.
    """

    try:

        # construct a BigQuery client object.
        client = bigquery.Client()

        # construct a full Dataset object to send to the API.
        dataset_ref = f"{project_id}.{dataset_id}"
        logging.info(f"Creating dataset `{dataset_ref}` in location `{dataset_location}` (if not exists).")
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = dataset_location

        # send the dataset to the API for creation (exists_ok=True used if dataset already exists)
        client.create_dataset(
            dataset,
            exists_ok=True
        )
        logging.info(f"Dataset `{dataset_ref}` created or already exists.")

    except Exception as e:
        logging.error(f"Error creating dataset `{project_id}.{dataset_id}`: {e}")
        raise