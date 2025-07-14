
from google.cloud import run_v2
from google.protobuf.json_format import MessageToDict
from typing import (
    Dict,
    List,
)

def get_cloud_run_jobs(
    project_id: str,
    region: str
)-> List[Dict]:
    """
    Arguments:
    - project_id: Google Cloud project ID
    - region: GOogle Cloud region

    Returns Cloud Run jobs found in project/region.
    """

    client = run_v2.JobsClient()
    parent = f"projects/{project_id}/locations/{region}"
    jobs = client.list_jobs(parent=parent)
    
    # convert to list of dicts
    job_list = []
    for job in jobs:
        job_dict = MessageToDict(
            job._pb,  # access the protobuf object
            preserving_proto_field_name=True  # keeps field names as in proto
        )
        job_list.append(job_dict)

    return job_list