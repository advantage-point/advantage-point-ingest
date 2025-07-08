import json
import pandas as pd

def safe_convert_value(val):
    """
    Arguments:
    - val: (column) value
    Safely converts values for BigQuery ingestion:
    - dict or list → JSON string (using json.dumps)
    - None or NaN → remains as None
    - Everything else → string

    This ensures all values in the column are strings (or None), avoiding mixed types.
    """
    try:

        if isinstance(val, (dict, list)):
            # Convert dictionaries and lists to JSON strings
            return json.dumps(val)
        elif isinstance(val, bytes):
            try:
                return val.decode('utf-8')
            except Exception:
                return str(val)  # Fallback to string repr
        elif pd.isna(val):
            # Keep nulls (NaN, None) unchanged
            return None
        else:
            # Convert all other types (numbers, bools, strings) to strings
            return str(val)

    except Exception as e:
        logging.error(f"Error converting value `{val}`: {e}")
        return str(val)
