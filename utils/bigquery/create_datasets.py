from google.cloud import bigquery
from utils.bigquery.create_dataset import create_dataset
from utils.bigquery.format_query_results import format_query_results
from utils.google_cloud.get_current_project_id import get_current_project_id
import logging

def create_datasets():
    """
    Creates BigQuery datasets if it does not exist.
    """

    try:

        # get project id
        project_id = get_current_project_id()

        # construct a BigQuery client object.
        client = bigquery.Client()

        # query control object for datasets

        datasets_sql = f"""
            WITH
                -- select target datasets
                target_datasets as (
                    SELECT DISTINCT
                        bigquery_target_project_id as bigquery_project_id,
                        bigquery_target_dataset_id as bigquery_dataset_id,
                        bigquery_target_dataset_location as bigquery_dataset_location
                    FROM {project_id}.meta.control_object__ingest__master
                    WHERE is_active = true
                ),
                -- select temp datasets
                temp_datasets as (
                    SELECT DISTINCT
                        bigquery_temp_project_id as bigquery_project_id,
                        bigquery_temp_dataset_id as bigquery_dataset_id,
                        bigquery_temp_dataset_location as bigquery_dataset_location
                    FROM {project_id}.meta.control_object__ingest__master
                    WHERE is_active = true
                ),
                -- union
                datasets_union as (
                    SELECT DISTINCT
                        *
                    FROM (
                        (SELECT * FROM target_datasets)
                        UNION ALL
                        (SELECT * FROM temp_datasets)
                    ) AS d
                )

                select * from datasets_union
                ;
        """
        # logging.info(f"Querying master control object for datasets: {datasets_sql}")
        datasets_sql_job = client.query(datasets_sql)
        datasets_list = format_query_results(query_job=datasets_sql_job)

        # loop through datasets and create
        for dataset_dict in datasets_list:

            # parse dict
            project_id = dataset_dict['bigquery_project_id']
            dataset_id = dataset_dict['bigquery_dataset_id']
            dataset_location = dataset_dict['bigquery_dataset_location']

            # create dataset
            logging.info(f"Checking existence of `{project_id}.{dataset_id}` in ({dataset_location}).")
            create_dataset(
                project_id=project_id,
                dataset_id=dataset_id,
                dataset_location=dataset_location
            )

    except Exception as e:
        logging.error(f"Error with datasets creation: {e}.")
        raise