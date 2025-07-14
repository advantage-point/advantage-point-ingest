from google.cloud import run_v2
import logging
from typing import Dict

def update_cloud_run_job(
    project_id: str,
    region: str,
    job_name: str,  # full resource path: projects/.../jobs/...
    job_payload: Dict
):
    """
    Updates an existing Cloud Run job with new configuration.

    Arguments:
    - project_id: Google Cloud project ID
    - region: Google Cloud region
    - job_name: Full Cloud Run job name (resource path)
    - job_payload: Dictionary of updated job configuration
    """
    try:
        # Initialize Cloud Run Jobs client
        client = run_v2.JobsClient()

        # Build the Job object with updated configuration
        job = run_v2.Job(
            name=job_name,
            template=run_v2.ExecutionTemplate(
                task_count=job_payload["task_count"],
                template=run_v2.TaskTemplate(
                    containers=[
                        run_v2.Container(
                            image=job_payload["template"]["containers"][0]["image"],
                            command=job_payload["template"]["containers"][0]["command"],
                            args=job_payload["template"]["containers"][0]["args"],
                        )
                    ],
                    max_retries=job_payload["template"]["max_retries"],
                    timeout=job_payload["template"]["timeout"],
                    service_account=job_payload["template"]["service_account"],
                )
            )
        )

        # Call update_job
        operation = client.update_job(job=job)

        # Wait for operation to complete
        logging.info(f"Updating Cloud Run job {job_name} in {project_id}/{region}. Waiting for completion...")
        result = operation.result()
        logging.info(f"Job {job_name} updated successfully in {project_id}/{region}.")

    except Exception as e:
        logging.error(f"Error when updating job {job_name}: {e}.")
