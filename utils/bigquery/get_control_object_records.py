from google.cloud import bigquery
from typing import (
    Dict,
    List,
)
from utils.bigquery.format_query_results import format_query_results

def get_control_object_records(
    project_id: str,
    column_name_list: List[str] = ['*'],
    where_clause_list: List[str] = ['1=1',]
) -> List[Dict]:
    """
    Arguments:
    - project_id: Google Cloud project ID
    - column_name_list: (optional) List of column names to select
    - where_clause_list: (optional) List of WHERE clauses

    Queries master control object for records.
    """
    client = bigquery.Client()

    # concatenate lists
    column_name_concat = ', '.join(column_name_list)
    where_clause_concat = ' AND '.join(where_clause_list)
    query_sql = f"""
        SELECT
            {column_name_concat}
        FROM {project_id}.meta.control_object__ingest__master
        WHERE {where_clause_concat}
        ;
    """
    query_sql_job = client.query(query_sql)
    return format_query_results(query_job=query_sql_job)