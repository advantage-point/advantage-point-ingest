from google.cloud import scheduler_v1
from google.protobuf import field_mask_pb2
from typing import Dict
import logging


def update_cloud_scheduler_job(
    project_id: str,
    region: str,
    job_name: str,
    job_payload: Dict
):
    """
    Arguments:
    - project_id: GCP project ID
    - region: GCP region
    - job_name: Full job name (projects/{project_id}/locations/{region}/jobs/{job_id})
    - job_payload: Dict of job config with required fields

    Updates an existing Cloud Scheduler job.
    """
    try:
        client = scheduler_v1.CloudSchedulerClient()

        # Build updated Job object
        job = scheduler_v1.Job(
            name=job_name,
            schedule=job_payload["schedule"],
            time_zone=job_payload["time_zone"],
            http_target=scheduler_v1.HttpTarget(
                uri=job_payload["http_target"]["uri"],
                http_method=scheduler_v1.HttpMethod[job_payload["http_target"]["http_method"]],
                headers=job_payload["http_target"]["headers"],
                oauth_token=scheduler_v1.OAuthToken(
                    service_account_email=job_payload["http_target"]["oauth_token"]["service_account_email"]
                )
            ),
            retry_config=scheduler_v1.RetryConfig(
                max_retry_duration=job_payload["retry_config"]["max_retry_duration"],
                min_backoff_duration=job_payload["retry_config"]["min_backoff_duration"],
                max_backoff_duration=job_payload["retry_config"]["max_backoff_duration"],
                max_doublings=job_payload["retry_config"]["max_doublings"]
            ),
            attempt_deadline=job_payload["attempt_deadline"]
        )

        # Define which fields to update
        update_mask = field_mask_pb2.FieldMask(paths=[
            "schedule",
            "time_zone",
            "http_target",
            "retry_config",
            "attempt_deadline"
        ])

        client.update_job(
            request={
                "job": job,
                "update_mask": update_mask
            }
        )

        logging.info(f"Successfully updated Cloud Scheduler job {job_name}.")

    except Exception as e:
        logging.error(f"Error updating Cloud Scheduler job {job_name}: {e}")
