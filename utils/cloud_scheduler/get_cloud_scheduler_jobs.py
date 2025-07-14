from google.cloud import scheduler_v1
from google.protobuf.json_format import MessageToDict
from typing import List, Dict

def get_cloud_scheduler_jobs(
    project_id: str,
    region: str
) -> List[Dict]:
    """
    Arguments:
    - project_id: Google Cloud project ID
    - region: Google Cloud region

    Returns a list of Cloud Scheduler jobs in the specified project and region as dictionaries.
    """
    client = scheduler_v1.CloudSchedulerClient()
    parent = f"projects/{project_id}/locations/{region}"

    jobs = client.list_jobs(request={"parent": parent})

    # convert to list of dicts
    job_list = []
    for job in jobs:
        job_dict = MessageToDict(
            job._pb,  # access the protobuf object
            preserving_proto_field_name=True  # keeps field names as in proto
        )
        job_list.append(job_dict)

    return job_list