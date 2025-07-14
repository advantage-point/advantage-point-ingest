from google.cloud import scheduler_v1
from google.protobuf import field_mask_pb2
import logging

def set_cloud_scheduler_job_state(
    job_name: str,
    state: str  # should be either "ENABLED" or "DISABLED"
) -> None:
    """
    Updates only the 'state' field of a Cloud Scheduler job.

    Args:
        job_name (str): Full job name (e.g., projects/.../locations/.../jobs/...)
        state (str): Desired job state: "ENABLED" or "DISABLED"
    """
    assert state in ["ENABLED", "DISABLED"], "State must be 'ENABLED' or 'DISABLED'"

    client = scheduler_v1.CloudSchedulerClient()

    try:
        job = scheduler_v1.Job(name=job_name, state=scheduler_v1.Job.State[state])
        mask = field_mask_pb2.FieldMask(paths=["state"])

        response = client.update_job(
            request={
                "job": job,
                "update_mask": mask
            }
        )

        logging.info(f"Set state of {job_name} to {state}.")

    except Exception as e:
        logging.error(f"Failed to set job state to {state} for {job_name}: {e}")
        raise
