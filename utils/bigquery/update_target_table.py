from google.cloud import bigquery
from utils.bigquery.drop_table import drop_table
from typing import List
import logging

def update_target_table(
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
    For a given record in the target table, if there is an updated version of that record in the source table:
    - expire the current target table record
    - insert the updated source table record as a new record in the target table
    """

    try:
        logging.info(f"Running SCD Type II update on `{target_project_id}.{target_dataset_id}.{target_table_id}`")

        # initialize client
        client = bigquery.Client()

        # create alias variables (for use in SQL statements)
        target_alias = 'TGT'
        source_alias = 'SRC'
        compare_alias = 'COMPARE'

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
        source_column_str = ',\n'.join(source_column_list)
        source_column_str_w_source_alias = ',\n'.join(
            [
                f"{source_alias}.{col}" for col in source_column_list
            ]
        )
        unique_column_join_str = ' AND '.join([
            f"{target_alias}.{unique_column_name} = {source_alias}.{unique_column_name}"
            for unique_column_name in unique_column_name_list
        ])
        non_unique_column_name_list = list(filter(lambda col: col not in unique_column_name_list, source_column_list))       
        non_unique_column_where_clause = '(' + '\nOR '.join(
            [
                f"SAFE_CAST({source_alias}.{col} AS STRING) != SAFE_CAST({target_alias}.{col} AS STRING)"
                for col in non_unique_column_name_list
            ]
        ) + ')'
        update_unique_column_where_clause = ' AND '.join([
            f"{target_alias}.{unique_column_name} = {compare_alias}.{unique_column_name}"
            for unique_column_name in unique_column_name_list
        ])

        # create table to store updated rows
        update_project_id = source_project_id
        update_dataset_id = source_dataset_id
        update_table_id = f"{source_table_id}__update"
        drop_table(
            project_id=update_project_id,
            dataset_id=update_dataset_id,
            table_id=update_table_id
        )
        create_update_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {update_project_id}.{update_dataset_id}.{update_table_id} AS
                WITH
                    -- select target table columns
                    -- select active records
                    -- EXCLUDE audit columns
                    {target_alias} AS (
                        SELECT
                            {source_column_str}
                        FROM {target_project_id}.{target_dataset_id}.{target_table_id}
                        WHERE audit_column__active_flag = true
                    ),
                    -- select source table columns
                    {source_alias} AS (
                        SELECT
                            {source_column_str}
                        FROM {source_project_id}.{source_dataset_id}.{source_table_id}
                    ),
                    -- join source and target rows
                    JOINED AS (
                        SELECT
                            {source_column_str_w_source_alias}
                        FROM {target_alias}
                        INNER JOIN {source_alias} ON {unique_column_join_str}
                        WHERE {non_unique_column_where_clause}
                    )
                SELECT * FROM JOINED
            ;
        """
        logging.info(f"Creating update table `{update_project_id}.{update_dataset_id}.{update_table_id}`.")
        create_update_table_sql_job = client.query(create_update_table_sql)
        create_update_table_sql_job.result()

        # handle updates for current records
        update_existing_sql = f"""
            UPDATE {target_project_id}.{target_dataset_id}.{target_table_id} AS {target_alias}
            SET
                    audit_column__active_flag = false,
                    audit_column__end_datetime_utc = current_timestamp,
                    audit_column__update_datetime_utc = current_timestamp
                FROM {update_project_id}.{update_dataset_id}.{update_table_id} AS {compare_alias}
                WHERE 1=1
                    AND {target_alias}.audit_column__active_flag = true
                    AND {update_unique_column_where_clause}
            ;
        """
        logging.info("Deactivating existing matching records.")
        update_existing_sql_job = client.query(update_existing_sql)
        update_existing_sql_job.result()

        # handle updates for new records
        update_new_sql = f"""
            INSERT INTO {target_project_id}.{target_dataset_id}.{target_table_id}
            ({source_column_str}, audit_column__active_flag, audit_column__record_type, audit_column__start_datetime_utc, audit_column__insert_datetime_utc)
            SELECT
                {source_column_str},
                true AS audit_column__active_flag,
                'update' AS audit_column__record_type,
                current_timestamp AS audit_column__start_datetime_utc,
                current_timestamp AS audit_column__insert_datetime_utc
            FROM {update_project_id}.{update_dataset_id}.{update_table_id}
            ;
        """
        logging.info("Inserting updated records.")
        update_new_sql_job = client.query(update_new_sql)
        update_new_sql_job.result()

        # drop the update table
        logging.info(f"Dropping update table.")
        drop_table(
            project_id=update_project_id,
            dataset_id=update_dataset_id,
            table_id=update_table_id
        )

        logging.info("SCD update completed successfully.")

    except Exception as e:
        logging.error(f"Failed to update target table `{target_project_id}.{target_dataset_id}.{target_table_id}`: {e}")
        raise