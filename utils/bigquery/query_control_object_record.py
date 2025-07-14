from google.cloud import bigquery
from typing import (
    Dict,
)
from utils.bigquery.format_query_results import format_query_results
from utils.google_cloud.get_current_project_id import get_current_project_id
import logging

def query_control_object_record(
    target_table_id: str
) -> Dict:
    """
    Arguments:
    - target_table_id: Target table id

    Queries/combines control object data for target table record (regardless of is_active).
    """

    try:

        # get project id
        project_id = get_current_project_id()

        # Construct a BigQuery client object.
        client = bigquery.Client()

        # query master control object
        control_object_master_sql = f"""
            SELECT
                *
            FROM {project_id}.meta.control_object__ingest__master
            WHERE bigquery_target_table_id = '{target_table_id}'
            ;
        """
        logging.info(f"Running master control object query: {control_object_master_sql}")
        control_object_master_sql_job = client.query(control_object_master_sql)
        control_object_master_dict = format_query_results(query_job=control_object_master_sql_job)[0]

        # query control table view
        id = control_object_master_dict['id']
        control_table_view_id_fq = control_object_master_dict['control_table_view_id_fq']
        control_table_table_id_fq = control_object_master_dict['control_table_table_id_fq']
        control_table_view_sql = f"""
            SELECT
                *
            FROM {control_table_view_id_fq}
            WHERE 1=1
                AND id = '{id}'
                AND control_table_table_id_fq = '{control_table_table_id_fq}'
            ;
        """
        logging.info(f"Running control table view query: {control_table_view_sql}")
        control_table_view_sql_job = client.query(control_table_view_sql)
        control_table_view_dict = format_query_results(query_job=control_table_view_sql_job)[0]

        # query control table table
        control_table_table_sql = f"""
            SELECT
                *
            FROM {control_table_table_id_fq}
            WHERE id = '{id}'
            ;
        """
        logging.info(f"Running control table table query: {control_table_table_sql}")
        control_table_table_sql_job = client.query(control_table_table_sql)
        control_table_table_dict = format_query_results(query_job=control_table_table_sql_job)[0]

        # combine dicts
        # overwrite order should be: master control object > control table view > control table table
        control_object = {
            **control_table_table_dict,
            **control_table_view_dict,
            **control_object_master_dict,
        }

        return control_object

    except Exception as e:
        logging.error(f"Failed to return control object: {e}")
        return {}