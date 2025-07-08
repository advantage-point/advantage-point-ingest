
from google.cloud import bigquery
import logging

def drop_table(
    project_id: str,
    dataset_id: str,
    table_id: str
):
    """
    Arguments:
    - project_id: Google Cloud project ID
    - dataset_id: Dataset name
    - table_id: Table name

    Drop table if it exists.
    """

    try:
        
        # Construct a BigQuery client object.
        client = bigquery.Client()

        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        logging.info(f"Attempting to drop table `{table_ref}` if it exists.")

        # drop table if exists (not_found_ok=True used if table does not exist)
        client.delete_table(
            table_ref,
            not_found_ok=True
        )

        logging.info(f"Table `{table_ref}` dropped (or did not exist).")

    except Exception as e:
        logging.error(f"Failed to drop table `{table_ref}`: {e}")
        raise