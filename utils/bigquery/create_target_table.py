
from google.cloud import bigquery
from utils.bigquery.create_table_with_df import create_table_with_df
import logging
import pandas as pd

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
        logging.error(f"Error during target table creation and/or audit column update: {e}.")
        raise