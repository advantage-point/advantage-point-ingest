from google.cloud import bigquery
from typing import (
    Dict,
)

def create_schema_field(
        schema_field_dict: Dict
) -> str:
    """

    See docs for more info: https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.schema.SchemaField.html

    Arguments:
    - schema_field_dict: Dictionary of SchemaField properties

    Returns a BigQuery SchemaField object
    """

    schema_field = bigquery.SchemaField(**schema_field_dict)

    return schema_field

