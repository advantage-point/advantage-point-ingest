from google.cloud import run_v2
from typing import (
    Dict,
)
import logging

def create_cloud_run_job(
    project_id: str,
    region: str,
    job_id: str,
    job_payload: Dict
):
    """
    Arguments:
    - project_id: Google Cloud project ID
    - region: Google Cloud region
    - job_id: Cloud Run job id
    - job_payload: Cloud Run job config

    Creates a new Cloud Run job using the given config payload.
    """

    try:
        # Initialize Cloud Run Jobs client
        client = run_v2.JobsClient()

        # Build parent resource string for the API call
        parent = f"projects/{project_id}/locations/{region}"

        # Define the Job object based on the input payload
        job = run_v2.Job(
            template=run_v2.ExecutionTemplate(
                task_count=job_payload["task_count"],  # Number of tasks per execution
                template=run_v2.TaskTemplate(
                    containers=[
                        run_v2.Container(
                            image=job_payload["template"]["containers"][0]["image"],  # Container image
                            command=job_payload["template"]["containers"][0]["command"],  # Entrypoint command
                            args=job_payload["template"]["containers"][0]["args"]  # Command arguments
                        )
                    ],
                    max_retries=job_payload["template"]["max_retries"],  # Retry count
                    timeout=job_payload["template"]["timeout"],  # Execution timeout
                    service_account=job_payload["template"]["service_account"]  # IAM service account
                )
            )
        )

        # Call the API to create the job
        operation = client.create_job(
            parent=parent,
            job=job,
            job_id=job_id
        )

        # Wait for the long-running operation to complete
        logging.info(f"Creating Cloud Run job {job_id} in {project_id}/{region}. Waiting for completion...")
        result = operation.result()
        logging.info(f"Job {job_id} created successfully in {project_id}/{region}.")

    except Exception as e:
        # Log any error encountered during job creation
        logging.error(f"Error when creating job {job_id}: {e}.")