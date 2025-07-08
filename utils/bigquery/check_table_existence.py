from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import logging

def check_table_existence(
    project_id: str,
    dataset_id: str,
    table_id: str
) -> bool:
    """
    Arguments:
        project_id: Google Cloud project ID
        dataset_id: Dataset name
        table_id: Table name

    Returns:
        True if the table exists, False otherwise
    """

    client = bigquery.Client()
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        logging.info(f"Checking existence of table: {table_ref}.")
        client.get_table(table_ref)
        logging.info(f"Table exists: {table_ref}")
        return True
    except NotFound:
        logging.info(f"Table does not exist: {table_ref}.")
        return False
