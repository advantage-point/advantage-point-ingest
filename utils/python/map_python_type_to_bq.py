def map_python_type_to_bq(
        data_type
) -> str:
    """
    Arguments:
    - data_type: Python data type

    Return the BigQuery data type equivalent
    """

    py_to_bq_dict = {
        'bool': 'BOOL',
        'datetime64[ns]': 'TIMESTAMP',
        'float64': 'FLOAT64',
        'int64': 'INT64',
        'object': 'STRING',
        'string': 'STRING',
    }

    # normalize data type in case weird format
    data_type_norm = str(data_type).lower()

    return py_to_bq_dict.get(data_type_norm, 'STRING')

