from google.protobuf import duration_pb2


def seconds_to_duration(duration_seconds: int) -> duration_pb2.Duration:
    """
    Arguments:
    - duration_seconds: Duration in seconds

    Converts an integer number of seconds into a protobuf Duration object.
    """
    return duration_pb2.Duration(seconds=duration_seconds)
