from google.cloud import bigquery
from typing import List
import logging

def insert_target_table(
    target_project_id: str,
    target_dataset_id: str,
    target_table_id: str,
    source_project_id: str,
    source_dataset_id: str,
    source_table_id: str,
    unique_column_name_list: List[str]
):
    """
    Arguments:
    - target_project_id: Google Cloud project ID for target_table_id
    - target_dataset_id: BigQuery dataset for target_table_id
    - target_table_id: BigQuery target table name
    - source_project_id: Google Cloud project ID for source_table_id
    - source_dataset_id: BigQuery dataset for source_table_id
    - source_table_id: BigQuery source table name
    - unique_column_name_list: List of columns that define uniqueness

    Compare records between source table and target table.
    Inserts records from source table that do not exist in the target table.
    """

    try:

        logging.info(f"Beginning insert of new records into `{target_project_id}.{target_dataset_id}.{target_table_id}`.")

        # initialize client
        client = bigquery.Client()

        # create alias variables (for use in SQL statements)
        target_alias = 'TGT'
        source_alias = 'SRC'

        # get list of columns from source table (for use in UPDATE statements)
        source_column_sql = f"""
            SELECT
                COLUMN_NAME
            FROM {source_project_id}.{source_dataset_id}.INFORMATION_SCHEMA.COLUMNS
            WHERE 1=1
                AND TABLE_SCHEMA = '{source_dataset_id}'
                AND TABLE_NAME = '{source_table_id}'
        """
        source_column_sql_job = client.query(source_column_sql)
        source_column_list = [row[0] for row in source_column_sql_job.result()]

        # generate lists/strings for unique/nonunique columns
        unique_column_join_str = ' AND '.join([
            f"{source_alias}.{unique_column_name} = {target_alias}.{unique_column_name}"
            for unique_column_name in unique_column_name_list
        ])
        source_column_str = ',\n'.join(source_column_list)
        source_column_str_w_source_alias = ',\n'.join(
            [
                f"{source_alias}.{col}" for col in source_column_list
            ]
        )

        # handle inserts for new records
        insert_new_sql = f"""
            INSERT INTO {target_project_id}.{target_dataset_id}.{target_table_id}
            ({source_column_str}, audit_column__active_flag, audit_column__record_type, audit_column__start_datetime_utc, audit_column__insert_datetime_utc)
            SELECT
                {source_column_str_w_source_alias},
                true AS audit_column__active_flag,
                'insert' AS audit_column__record_type,
                current_timestamp AS audit_column__start_datetime_utc,
                current_timestamp AS audit_column__insert_datetime_utc
            FROM {source_project_id}.{source_dataset_id}.{source_table_id} AS {source_alias}
            LEFT JOIN {target_project_id}.{target_dataset_id}.{target_table_id} AS {target_alias} ON {unique_column_join_str}
            WHERE {target_alias}.{unique_column_name_list[0]} IS NULL
            ;
        """
        insert_new_sql_job = client.query(insert_new_sql)
        insert_new_sql_job.result()
        logging.info("Insert complete: new records added.")

    except Exception as e:
        logging.error(f"Error inserting new records: {e}")
        raise
    