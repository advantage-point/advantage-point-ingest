
from google.cloud import bigquery
from utils.bigquery.create_table_with_df import create_table_with_df
from utils.bigquery.create_table_with_cloud_storage import create_table_with_cloud_storage
import logging
import pandas as pd

def add_audit_columns(
    project_id: str,
    dataset_id: str,
    table_id: str
):
    """
    Arguments:
    - project_id: Google Cloud project ID
    - dataset_id: Dataset name
    - table_id: Table name

    Add audit columns to table.
    """

    try:

        # Construct a BigQuery client object.
        client = bigquery.Client()

        # add audit columns
        add_audit_columns_sql = f"""
            ALTER TABLE {project_id}.{dataset_id}.{table_id}
            ADD COLUMN audit_column__active_flag BOOLEAN,
            ADD COLUMN audit_column__record_type STRING,
            ADD COLUMN audit_column__start_datetime_utc TIMESTAMP,
            ADD COLUMN audit_column__end_datetime_utc TIMESTAMP,
            ADD COLUMN audit_column__insert_datetime_utc TIMESTAMP,
            ADD COLUMN audit_column__update_datetime_utc TIMESTAMP,
            ADD COLUMN audit_column__delete_datetime_utc TIMESTAMP
            ;
        """
        
        try:
            client.query(add_audit_columns_sql).result()
            logging.info("Audit columns added.")
        except Exception as e:
            logging.warning(f"Audit columns may already exist or failed to add: {e}")

        # update audit column values
        update_audit_columns_sql = f"""
            UPDATE {project_id}.{dataset_id}.{table_id}
            SET
                audit_column__active_flag = true,
                audit_column__record_type = 'insert',
                audit_column__start_datetime_utc = current_timestamp,
                audit_column__insert_datetime_utc = current_timestamp
            WHERE 1=1
            ;
        """
        client.query(update_audit_columns_sql).result()
        logging.info("Audit column values updated.")

    except Exception as e:
        logging.error(f"Error during target table audit column update: {e}.")
        raise

def create_target_table(
    project_id: str,
    dataset_id: str,
    table_id: str,
    dataframe: pd.DataFrame
):
    """
    Arguments:
    - project_id: Google Cloud project ID
    - dataset_id: Dataset name
    - table_id: Table name
    - dataframe: Pandas dataframe

    Creates/loads dataframe data to target table and adds 'insert' values for audit columns.
    """

    try:

        logging.info(f"Creating/loading target table `{project_id}.{dataset_id}.{table_id}`.")

        # create target table
        create_table_with_df(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            dataframe=dataframe
        )

        add_audit_columns(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id
        )

    except Exception as e:
        logging.error(f"Error during target table creation and/or audit column update: {e}.")
        raise

def create_target_table_with_cloud_storage(
    cloudstorage_bucket_name: str,
    cloudstorage_object_pattern: str,
    bigquery_project_id: str,
    bigquery_dataset_id: str,
    bigquery_dataset_location: str,
    bigquery_table_id: str
) -> None:
    
    """
    Arguments:
        cloudstorage_bucket_name: Cloud Storage bucket name
        cloudstorage_object_pattern: Cloud Storage object pattern (e.g. 'tmp/matches/20250722/')
        bigquery_project_id: BigQuery Project ID
        bigquery_dataset_id: BigQuery dataset ID
        bigquery_table_id: BigQuery table ID

    Loads all GCS files with a specific extension under a prefix into a BigQuery `target` table and adds audit columns.
    """

    try:

        # create/load target table
        create_table_with_cloud_storage(
            cloudstorage_bucket_name=cloudstorage_bucket_name,
            cloudstorage_object_pattern=cloudstorage_object_pattern,
            bigquery_project_id=bigquery_project_id,
            bigquery_dataset_id=bigquery_dataset_id,
            bigquery_dataset_location=bigquery_dataset_location,
            bigquery_table_id=bigquery_table_id
        )
        logging.info(f"Loaded data from Cloud Storage to Bigquery table ({bigquery_project_id}.{bigquery_dataset_id}.{bigquery_table_id}).")

        add_audit_columns(
            project_id=bigquery_project_id,
            dataset_id=bigquery_dataset_id,
            table_id=bigquery_table_id
        )

    except Exception as e:
        logging.error(f"Error during target table creation and/or audit column update: {e}.")
        raise
