
# from google.cloud import bigquery
# from utils.bigquery.safe_convert_value import safe_convert_value
import logging
import pandas as pd
import pandas_gbq

def create_table_with_df(
    project_id: str,
    dataset_id: str,
    table_id: str,
    dataframe: pd.DataFrame
):
    """
    Arguments:
    - project_id: Google Cloud project ID
    - dataset_id: Dataset name
    - table_id: Table name
    - dataframe: Pandas dataframe

    Loads dataframe data to table (table is created if it does not exist).
    """

    try:

        logging.info(f"Preparing to load data to `{project_id}.{dataset_id}.{table_id}`.")

        # Ensure column names are strings
        # dataframe.columns = dataframe.columns.astype(str)

        # Replace NaNs with None and convert everything else to string
        dataframe = dataframe.where(pd.notna(dataframe), None).astype(str)

        pandas_gbq.to_gbq(
            dataframe,
            destination_table=f"{dataset_id}.{table_id}",
            project_id=project_id
        )   

        # # Convert to object dtype first (allows mixed types)
        # dataframe = dataframe.astype(object)

        # # Apply safe conversion per value
        # for col in dataframe.columns:
        #     dataframe[col] = dataframe[col].map(safe_convert_value).astype(str)
            
        # # Construct a BigQuery client object.
        # client = bigquery.Client()

        # # create job to load dataframe into table (WRITE_APPEND will insert rows without overwriting)
        # job_config = bigquery.LoadJobConfig(
        #     write_disposition="WRITE_APPEND"
        # )
        # job = client.load_table_from_dataframe(
        #     dataframe=dataframe,
        #     destination=f"{project_id}.{dataset_id}.{table_id}",
        #     job_config=job_config
        # )
        # job.result()

        logging.info(f"Successfully loaded {len(dataframe)} rows to `{project_id}.{dataset_id}.{table_id}`.")

    except Exception as e:
        logging.error(f"Failed to load data to `{project_id}.{dataset_id}.{table_id}`: {e}")
        raise