from google.cloud import bigquery
from typing import (
    List,
)
from utils.bigquery.format_query_results import format_query_results
import logging

def alter_target_table(
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

    Compares source and target table schemas in BigQuery and generates SQL ALTER TABLE statements.
    """

    logging.info(f"Starting schema comparison for target table `{target_project_id}.{target_dataset_id}.{target_table_id}`.")

    # initialize client
    client = bigquery.Client()
    
    columns_compare_sql = f"""
    WITH
        source_columns AS (
        SELECT
            column_name,
            data_type
        FROM {source_project_id}.{source_dataset_id}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{source_table_id}'
        ),
        
        target_columns AS (
        SELECT
            column_name,
            data_type,
        FROM {target_project_id}.{target_dataset_id}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{target_table_id}'
            AND column_name NOT IN (
            'audit_column__active_flag',
            'audit_column__record_type',
            'audit_column__start_datetime_utc',
            'audit_column__end_datetime_utc',
            'audit_column__insert_datetime_utc',
            'audit_column__update_datetime_utc',
            'audit_column__delete_datetime_utc'
            )
        ),
        
        join_columns AS (
            SELECT
                s.column_name as source_column_name,
                s.data_type AS source_data_type,
                t.column_name as target_column_name,
                t.data_type AS target_data_type,
                case
                    when  s.column_name is not null and t.column_name is null then 'add'
                    -- see details for alter: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#details_21
                    when (
                        (s.data_type in ('INT64') AND t.data_type in ('NUMERIC', 'BIGNUMERIC', 'FLOAT64'))
                        or (s.data_type in ('NUMERIC') and t.data_type in ('BIGNUMERIC', 'FLOAT64'))
                    ) then 'alter'
                    when s.data_type != t.data_type then 'complex_alter'
                    else null
                end as comparison_type
            FROM source_columns s
            FULL OUTER JOIN target_columns t ON s.column_name = t.column_name
        )
    SELECT * FROM join_columns
    WHERE comparison_type is not null
    ;
    """
    
    logging.info("Comparing source and target schema columns.")
    columns_compare_sql_job = client.query(columns_compare_sql)
    alter_table_list = format_query_results(query_job=columns_compare_sql_job)
    logging.info(f"Schema comparison found {len(alter_table_list)} columns to modify.")

    # loop through list and construct/execute ALTER TABLE statements
    for alter_table_dict in alter_table_list:

        # initialize variable
        comparison_type = alter_table_dict['comparison_type']
        source_column_name = alter_table_dict['source_column_name']
        source_data_type = alter_table_dict['source_data_type']
        target_column_name = alter_table_dict['target_column_name']
        target_data_type = alter_table_dict['target_data_type']

        if comparison_type == 'add':
            logging.info(f"Adding new column `{source_column_name}` of type `{source_data_type}` to target table.")

            # add column
            alter_table_add_sql = f"""
                ALTER TABLE {target_project_id}.{target_dataset_id}.{target_table_id}
                ADD COLUMN {source_column_name} {source_data_type}
                ;
            """

            # update target table for new column
            target_alias = 'TGT'
            source_alias = 'SRC'
            update_unique_column_where_clause = ' AND '.join([
                f"{target_alias}.{unique_column_name} = {source_alias}.{unique_column_name}"
                for unique_column_name in unique_column_name_list
            ])
            alter_table_add_update_sql = f"""
                UPDATE {target_project_id}.{target_dataset_id}.{target_table_id} AS {target_alias}
                SET {source_column_name} = {source_alias}.{source_column_name}
                FROM {source_project_id}.{source_dataset_id}.{source_table_id} AS {source_alias}
                WHERE 1=1
                    AND {target_alias}.audit_column__active_flag = true
                    AND {update_unique_column_where_clause}
                ;
            """
            try:
                client.query(f"{alter_table_add_sql} {alter_table_add_update_sql}").result()
                logging.info(f"Successfully added and populated column `{source_column_name}`.")
            except Exception as e:
                logging.error(f"Failed to alter or update column `{source_column_name}`: {e}")

        elif comparison_type == 'alter':

            logging.info(f"Altering column `{target_column_name}` to data type `{source_data_type}`.")
            
            alter_column_alter_sql = f"""
                ALTER TABLE {target_project_id}.{target_dataset_id}.{target_table_id}
                ALTER COLUMN {target_column_name} SET DATA TYPE {source_data_type}
                ;
            """

            try:
                client.query(alter_column_alter_sql).result()
                logging.info(f"Successfully altered column `{target_column_name}`.")
            except Exception as e:
                logging.error(f"Failed to alter column `{target_column_name}`: {e}")

        elif comparison_type == 'complex_alter':
            logging.warning(
                f"Complex type change detected for column `{target_column_name}` "
                f"(target type: {target_data_type}, "
                f"source type: {source_data_type}). Skipping automatic change."
            )

        
    logging.info("Schema alignment complete.")