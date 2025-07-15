from google.cloud import scheduler_v1
from utils.bigquery.get_control_object_records import get_control_object_records
from utils.cloud_scheduler.cloud_scheduler_job_default_config import cloud_scheduler_job_default_config
from utils.google_cloud.convert_seconds_to_duration import convert_seconds_to_duration
from utils.cloud_scheduler.create_cloud_scheduler_job import create_cloud_scheduler_job
from utils.cloud_scheduler.get_cloud_scheduler_jobs import get_cloud_scheduler_jobs
from utils.cloud_scheduler.pause_cloud_scheduler_job import pause_cloud_scheduler_job
from utils.cloud_scheduler.update_cloud_scheduler_job import update_cloud_scheduler_job
from utils.google_cloud.get_current_project_id import get_current_project_id
import logging

def main():

    # set logging config
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    )

    project_id = get_current_project_id()

    control_object_record_list = get_control_object_records(
        project_id=project_id
    )

    # Get distinct (project_id, region) pairs from control object records
    project_region_pairs = {
        (record['cloudscheduler_project_id'], record['cloudscheduler_region'])
        for record in control_object_record_list
    }

    # Convert back to list of dicts
    project_region_list = [
        {'project_id': project_id, 'region': region}
        for (project_id, region) in project_region_pairs
    ]

    # get cloud scheduler jobs
    cloud_scheduler_job_list = []
    for project_region_dict in project_region_list:

        # parse dict
        project_id = project_region_dict['project_id']
        region = project_region_dict['region']

        # get cloud scheduler jobs
        cloud_scheduler_jobs = get_cloud_scheduler_jobs(
            project_id=project_id,
            region=region
        )
        cloud_scheduler_job_list.extend(cloud_scheduler_jobs)

    # get list of unique cloud scheduler prefixes
    cloud_scheduler_prefix_list = list(set([
        row['cloudscheduler_prefix'] for row in control_object_record_list
    ]))
    
    # filter cloud scheduler jobs to just ones that contain prefix
    cloud_scheduler_job_list = [
        job for job in cloud_scheduler_job_list
        if any(prefix in job['name'] for prefix in cloud_scheduler_prefix_list)
        and 'update-google-cloud' not in job['name'] # exclude the 'update-google-cloud' job
    ]

    # build lookup dictionaries
    control_lookup = {record['cloudscheduler_job_name']: record for record in control_object_record_list}
    cloud_scheduler_lookup = {job['name']: job for job in cloud_scheduler_job_list}
    all_job_names = set(control_lookup.keys()) | set(cloud_scheduler_lookup.keys())  # UNION of keys

    # loop through job names
    for job_name in all_job_names:

        # get job name/properties from lists
        control_record_dict = control_lookup.get(job_name)
        cloud_scheduler_job_dict = cloud_scheduler_lookup.get(job_name)

        # if no control record dict
        if not control_record_dict:
            logging.info(f"No control table record for {job_name}.")

            # pause any active jobs
            cloud_scheduler_job_state = cloud_scheduler_job_dict['state']
            if cloud_scheduler_job_state == 'ENABLED':
                logging.info(f"Pausing Cloud Scheduler job: {job_name}.")
                pause_cloud_scheduler_job(
                    job_name=job_name
                )
        
        # otherwise (implying that both records exist)
        else:

            # construct job from control record

            # job from create_job:
                # name
                # http_target
                    # uri
                    # http_method (POST)
                    # headers (can be null?)
                # schedule
                # time_zone
            
        
        # create if no existing cloud scheduler job
        elif control_record_dict and not cloud_scheduler_job_dict:
            pass


        else:

            project_id = control_record_dict['cloudscheduler_project_id']
            region = control_record_dict['cloudscheduler_region']
            # job_id = control_record_dict['cloudscheduler_job_id']
            # is_active = control_record_dict['is_active']
            # job_state = 'ENABLED' if is_active == True else 'PAUSED'

            
                
            
            config = cloud_scheduler_job_default_config.copy()
            config.update({
                "schedule": control_record_dict['cloudscheduler_frequency'],
                "time_zone": control_record_dict['cloudscheduler_timezone'],
                "uri": control_record_dict['cloudscheduler_http_target_uri'],
                "service_account_email": control_record_dict['cloudscheduler_service_account'],
            })

            job_payload = {
                "schedule": config["schedule"],
                "time_zone": config["time_zone"],
                "http_target": {
                    "uri": config["uri"],
                    "http_method": config["http_method"],
                    "headers": config["headers"],
                    "oauth_token": {
                        "service_account_email": config["service_account_email"]
                    }
                },
                "retry_config": {
                    "max_retry_duration": convert_seconds_to_duration(duration_seconds=config["retry_config"]["max_retry_duration_seconds"]),
                    "min_backoff_duration": convert_seconds_to_duration(duration_seconds=config["retry_config"]["min_backoff_duration_seconds"]),
                    "max_backoff_duration": convert_seconds_to_duration(duration_seconds=config["retry_config"]["max_backoff_duration_seconds"]),
                    "max_doublings": config["retry_config"]["max_doublings"]
                },
                "attempt_deadline": convert_seconds_to_duration(duration_seconds=config["attempt_deadline_seconds"]),
            }

            # if job name exists in control table but NOT cloud scheduler --> create it
            if control_record_dict and not cloud_scheduler_job_dict:

                job_created = False
                try:
                    create_cloud_scheduler_job(
                        project_id=project_id,
                        region=region,
                        job_id=job_id,
                        job_payload=job_payload
                    )
                    job_created = True
                except Exception as e:
                    logging.error(f"Create failed for {job_name}: {e}")

                # Only run state update if job exists or was just created
                if job_created:
                    try:
                        set_cloud_scheduler_job_state(
                            job_name=job_name,
                            state=job_state
                        )
                    except Exception as e:
                        logging.error(f"Set state failed for {job_name}: {e}")
                else:
                    logging.warning(f"Skipping state update — job not found or create failed for {job_name}.")


                # create cloud scheduler job
                logging.info(f"Creating Cloud Scheduler job {job_name} in {project_id}/{region}.")
                create_cloud_scheduler_job(
                    project_id=project_id,
                    region=region,
                    job_id=job_id,
                    job_payload=job_payload
                )

        
            # Check if properties are different
            elif control_record_dict and cloud_scheduler_job_dict:

                # Flatten nested Cloud Scheduler job fields for easier comparison
                deployed_config = {
                    "uri": cloud_scheduler_job_dict["http_target"]["uri"],
                    "http_method": cloud_scheduler_job_dict["http_target"].get("http_method", "POST"),
                    "headers": cloud_scheduler_job_dict["http_target"].get("headers", {}),
                    "service_account_email": cloud_scheduler_job_dict["http_target"]
                        .get("oauth_token", {})
                        .get("service_account_email", ""),
                    "schedule": cloud_scheduler_job_dict["schedule"],
                    "time_zone": cloud_scheduler_job_dict["time_zone"],
                    "retry_config": cloud_scheduler_job_dict.get("retry_config", {}),
                    "attempt_deadline": cloud_scheduler_job_dict.get("attempt_deadline", ""),
                }

                desired_config = {
                    "uri": job_payload["http_target"]["uri"],
                    "http_method": job_payload["http_target"]["http_method"],
                    "headers": job_payload["http_target"]["headers"],
                    "service_account_email": job_payload["http_target"]["oauth_token"]["service_account_email"],
                    "schedule": job_payload["schedule"],
                    "time_zone": job_payload["time_zone"],
                    "retry_config": job_payload["retry_config"],
                    "attempt_deadline": job_payload["attempt_deadline"],
                }

                if deployed_config != desired_config:

                    logging.info(f"Updating Cloud Scheduler job: {job_name}.")
                    update_cloud_scheduler_job(
                        project_id=project_id,
                        region=region,
                        job_name=job_name,
                        job_payload=job_payload
                    )

            # set cloud scheduler job state
            set_cloud_scheduler_job_state(
                job_name=job_name,
                state=job_state
            )

if __name__ == '__main__':
    main()

