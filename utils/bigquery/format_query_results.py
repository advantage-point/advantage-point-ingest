from google.cloud import bigquery
from typing import (
    Dict,
    List,
)
import logging

def format_query_results(
    query_job
) -> List[Dict]:
    """
    Arguments:
    - query_job: BigQuery job

    Formats BigQuery results (found in BigQuery job) as list of dictionaries
    """

    try:
        logging.info("Fetching results from BigQuery query job.")
        rows = query_job.result()
        results = [dict(row) for row in rows]
        return results

    except Exception as e:
        logging.error(f"Error formatting query results: {e}")
        raise