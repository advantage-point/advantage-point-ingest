from google.cloud import scheduler_v1
import logging

def pause_cloud_scheduler_job(
    job_name: str
) -> None:
    """
    See documentation for more details: https://cloud.google.com/python/docs/reference/cloudscheduler/latest/google.cloud.scheduler_v1.services.cloud_scheduler.CloudSchedulerClient#google_cloud_scheduler_v1_services_cloud_scheduler_CloudSchedulerClient_pause_job
    
    Arguments:
    - job_name: Cloud Schedule job name

    Pauses the Cloud Scheduler job.
    """


    try:

        # Create a client
        client = scheduler_v1.CloudSchedulerClient()

        # Initialize request argument(s)
        request = scheduler_v1.PauseJobRequest(
            name=job_name,
        )

        # Make the request
        response = client.pause_job(request=request)
        logging.info(f"Job {job_name} has been paused.")


    except Exception as e:
        logging.error(f"Error when pausing job {job_name}: {e}.")