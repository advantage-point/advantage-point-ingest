from google.cloud import scheduler_v1
from typing import Dict
import logging


def create_cloud_scheduler_job(
    project_id: str,
    region: str,
    job_id: str,
    job_payload: Dict
):
    """
    Arguments:
    - project_id: Google Cloud project ID
    - region: Google Cloud region
    - job_id: Cloud Scheduler job id
    - job_payload: Cloud Scheduler job configuration as a dictionary

    Creates a new Cloud Scheduler job using the given config.
    """
    try:
        for key, val in job_payload["retry_config"].items():
            logging.info(f"retry_config[{key}] = {val} ({type(val)})")
        logging.info("Attempt deadline type: %s", type(job_payload["attempt_deadline"]))

        client = scheduler_v1.CloudSchedulerClient()
        parent = f"projects/{project_id}/locations/{region}"

        # Build the Job object from the job_payload
        headers = {str(k): str(v) for k, v in job_payload["http_target"].get("headers", {}).items()}

        job = scheduler_v1.Job(
            schedule=job_payload["schedule"],
            time_zone=job_payload["time_zone"],
            http_target=scheduler_v1.HttpTarget(
                uri=job_payload["http_target"]["uri"],
                http_method = scheduler_v1.HttpMethod[job_payload["http_target"]["http_method"]],
                headers=headers,
                oauth_token=scheduler_v1.OAuthToken(
                    service_account_email=job_payload["http_target"]["oauth_token"]["service_account_email"]
                )
            ),
            retry_config=scheduler_v1.RetryConfig(
                max_retry_duration=job_payload["retry_config"]["max_retry_duration"],
                min_backoff_duration=job_payload["retry_config"]["min_backoff_duration"],
                max_backoff_duration=job_payload["retry_config"]["max_backoff_duration"],
                max_doublings=job_payload["retry_config"]["max_doublings"],
            ),
            attempt_deadline=job_payload["attempt_deadline"]
        )

        # Create the job
        response = client.create_job(
            request={
                "parent": parent,
                "job": job,
                "job_id": job_id
            }
        )
        logging.info(f"Cloud Scheduler job {job_id} created successfully in {project_id}/{region}.")

    except Exception as e:
        logging.error(f"Error when creating Cloud Scheduler job {job_id}: {e}")
        raise
