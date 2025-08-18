from google.cloud import bigquery
import logging

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