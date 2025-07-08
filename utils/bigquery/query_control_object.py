from google.cloud import bigquery
from typing import (
    Dict,
    List,
)
from utils.bigquery.format_query_results import format_query_results
from utils.google_cloud.get_current_project_id import get_current_project_id
import logging
import os

def query_control_object(
    project_id: str = get_current_project_id(),
    dataset_id: str = 'meta',
    control_object_id: str = os.getenv('CONTROL_TABLE_VIEW_NAME'),
    column_name_list: List[str] = ['*',],
    where_clause_list: List[str] = ['1 = 1',]
) -> List[Dict]:
    """
    Arguments:
    - project_id: Google Cloud project id for control object
    - dataset_id: Dataset for control object
    - control_object_id: Table/view that represents 'control table'
    - column_name_list: List of columns in SELECT statement
    - where_clause_list: List of WHERE clause conditions

    Queries control object for list of records.
    """

    logging.info(f"Querying control object for records.")

    try:

        # Construct a BigQuery client object.
        client = bigquery.Client()

        # concatenate lists
        column_name_str = ', '.join(column_name_list)
        where_clause_str = ' AND '.join(where_clause_list)

        # query control object
        control_object_query_sql = f"""
            SELECT
                {column_name_str}
            FROM {project_id}.{dataset_id}.{control_object_id}
            WHERE {where_clause_str}
            ;
        """
        logging.info(f"Running control object query: {control_object_query_sql}")
        control_object_query_sql_job = client.query(control_object_query_sql)
        control_object_list = format_query_results(query_job=control_object_query_sql_job)
        return control_object_list

    except Exception as e:
        logging.error(f"Failed to query control object: {e}")
        return []