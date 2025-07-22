from google.cloud import bigquery
import logging

def create_table_with_cloud_storage(
    cloudstorage_bucket_name: str,
    cloudstorage_object_pattern: str,
    bigquery_project_id: str,
    bigquery_dataset_id: str,
    bigquery_dataset_location: str,
    bigquery_table_id: str
) -> None:
    
    """
    Arguments:
        cloudstorage_bucket_name: Cloud Storage bucket name
        cloudstorage_object_pattern: Cloud Storage object pattern (e.g. 'tmp/matches/20250722/')
        bigquery_project_id: BigQuery Project ID
        bigquery_dataset_id: BigQuery dataset ID
        bigquery_table_id: BigQuery table ID

    Loads all GCS files with a specific extension under a prefix into a BigQuery table.
    """

    try:

        # Construct a BigQuery client object.
        client = bigquery.Client()

        # TODO(developer): Set table_id to the ID of the table to create.
        table_fq = f"{bigquery_project_id}.{bigquery_dataset_id}.{bigquery_table_id}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition='WRITE_APPEND'
        )

        cloudstorage_uri = f"gs://{cloudstorage_bucket_name}/{cloudstorage_object_pattern}"

        load_job = client.load_table_from_uri(
            cloudstorage_uri,
            table_fq,
            location=bigquery_dataset_location,  # Must match the destination dataset location.
            job_config=job_config,
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        logging.info(f"Loaded data from Cloud Storage URI ({cloudstorage_uri}) to Bigquery table ({table_fq})")

    except Exception as e:
        logging.error(f"Error loading data from Cloud Storage URI ({cloudstorage_uri}) to Bigquery table ({table_fq}): {e}.")