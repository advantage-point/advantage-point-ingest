from google.cloud import bigquery
from utils.bigquery.format_query_results import format_query_results
from utils.google_cloud.get_current_project_id import get_current_project_id
import logging

def main():

    try:

        # get project id
        project_id = get_current_project_id()

        # construct a BigQuery client object.
        client = bigquery.Client()

        # query control object
        control_object_sql = f"""
            SELECT
                *
            FROM {project_id}.meta.control_object__ingest__master
            ;
        """
        logging.info(f"Retrieving control object records.")
        control_object_sql_job = client.query(control_object_sql)
        control_object_list = format_query_results(query_job=control_object_sql_job)


    except Exception as e:
        logging.error(f"Error with cloud run update: {e}.")
        raise
    

if __name__ == '__main__':
    main()