from google.cloud import scheduler_v1
from google.protobuf.json_format import MessageToDict
from typing import List, Dict
import logging

def get_cloud_scheduler_jobs(
    project_id: str,
    region: str
) -> List[Dict]:
    """
    See docs for more info: https://cloud.google.com/python/docs/reference/cloudscheduler/latest/google.cloud.scheduler_v1.services.cloud_scheduler.CloudSchedulerClient#google_cloud_scheduler_v1_services_cloud_scheduler_CloudSchedulerClient_list_jobs

    Arguments:
    - project_id: Google Cloud project ID
    - region: Google Cloud region

    Returns a list of Cloud Scheduler jobs in the specified project and region as dictionaries.
    """

    parent = f"projects/{project_id}/locations/{region}"

    try:
        # Create a client
        client = scheduler_v1.CloudSchedulerClient()

        # Initialize request argument(s)
        request = scheduler_v1.ListJobsRequest(
            parent=parent,
        )

        # Make the request
        jobs = client.list_jobs(request=request)

        # convert to list of dicts
        job_list = []
        for job in jobs:
            job_dict = MessageToDict(
                job._pb,  # access the protobuf object
                preserving_proto_field_name=True  # keeps field names as in proto
            )
            job_list.append(job_dict)

        return job_list

    except Exception as e:
        logging.error(f"Error retrieving Cloud Scheduler jobs in parent {parent}")
        return []