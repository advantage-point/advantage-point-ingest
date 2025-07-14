from google.cloud import bigquery
from typing import (
    Dict,
    List,
)
from utils.bigquery.format_query_results import format_query_results
from utils.cloud_run.cloud_run_job_default_config import cloud_run_job_default_config
from utils.cloud_run.create_cloud_run_job import create_cloud_run_job
from utils.cloud_run.get_cloud_run_jobs import get_cloud_run_jobs
from utils.cloud_run.update_cloud_run_job import update_cloud_run_job
from utils.google_cloud.get_current_project_id import get_current_project_id
import logging

def fetch_control_table_records(
    project_id: str
) -> List[Dict]:
    """
    Arguments:
    - project_id: Google Cloud project ID

    Queries master control object for records.
    """
    client = bigquery.Client()
    query_sql = f"""
        SELECT
            *
        FROM {project_id}.meta.control_object__ingest__master
        ;
    """
    query_sql_job = client.query(query_sql)
    return format_query_results(query_job=query_sql_job)



def main():

    # set logging config
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    )

    project_id = get_current_project_id()

    control_object_record_list = fetch_control_table_records(
        project_id=project_id
    )

    # Get distinct (project_id, region) pairs from control object records
    project_region_pairs = {
        (record['cloudrun_project_id'], record['cloudrun_region'])
        for record in control_object_record_list
    }

    # Convert back to list of dicts
    project_region_list = [
        {'project_id': project_id, 'region': region}
        for (project_id, region) in project_region_pairs
    ]

    # get cloud run jobs
    cloud_run_job_list = []
    for project_region_dict in project_region_list:

        # parse dict
        project_id = project_region_dict['project_id']
        region = project_region_dict['region']

        # get cloud run jobs
        cloud_run_jobs = get_cloud_run_jobs(
            project_id=project_id,
            region=region
        )
        cloud_run_job_list.extend(cloud_run_jobs)

    # get list of unique cloud run prefixes
    cloud_run_prefix_list = list(set([
        row['cloudrun_prefix'] for row in control_object_record_list
    ]))
    
    # filter cloud run jobs to just ones that contain prefix
    cloud_run_job_list = [
        job for job in cloud_run_job_list
        if any(prefix in job['name'] for prefix in cloud_run_prefix_list)
        and 'orchestrate' not in job['name'] # exclude the 'orchestrate' job
    ]

    # build lookup dictionaries
    control_lookup = {record['cloudrun_job_name']: record for record in control_object_record_list}
    cloud_run_lookup = {job['name']: job for job in cloud_run_job_list}
    all_job_names = set(control_lookup.keys()) | set(cloud_run_lookup.keys())  # UNION of keys

    # loop through job names
    for job_name in all_job_names:

        # get job name/properties from lists
        control_record_dict = control_lookup.get(job_name)
        cloud_run_job_dict = cloud_run_lookup.get(job_name)

        # skip if no control record dict
        if not control_record_dict:
            logging.info(f"No control table record for {job_name}.")
            continue

        # otherwise create the job payload
        else:

            project_id = control_record_dict['cloudrun_project_id']
            region = control_record_dict['cloudrun_region']
            job_id = control_record_dict['cloudrun_job_id']
                
            
            config = cloud_run_job_default_config.copy()
            config.update({
                "task_count": control_record_dict["cloudrun_number_of_tasks"],
                "max_retries": control_record_dict["cloudrun_max_retries"],
                "timeout_seconds": control_record_dict["cloudrun_task_timeout_seconds"],
                "command": [control_record_dict["cloudrun_container_command"]],
                "args": control_record_dict["cloudrun_container_arguments_args_array"],
                "service_account": control_record_dict["cloudrun_security_service_account"],
                "image": control_record_dict["cloudrun_container_image_url"],
            })

            job_payload = {
                "task_count": config["task_count"],
                "template": {
                    "containers": [
                        {
                            "image": config["image"],
                            "command": config["command"],
                            "args": config["args"],
                        }
                    ],
                    "max_retries": config["max_retries"],
                    "timeout": {"seconds": config["timeout_seconds"]},
                    "service_account": config["service_account"],
                }
            }

            # if job name exists in control table but NOT cloud run --> create it
            if control_record_dict and not cloud_run_job_dict:

                # create cloud run job
                logging.info(f"Creating Cloud Run job {job_name} in {project_id}/{region}.")
                create_cloud_run_job(
                    project_id=project_id,
                    region=region,
                    job_id=job_id,
                    job_payload=job_payload
                )
        
            # check if properties are different
            elif control_record_dict and cloud_run_job_dict:

                # Flatten nested Cloud Run job fields for easier comparison
                deployed_config = {
                    "task_count": cloud_run_job_dict["template"]["task_count"],
                    "image": cloud_run_job_dict["template"]["template"]["containers"][0]["image"],
                    "command": cloud_run_job_dict["template"]["template"]["containers"][0].get("command", []),
                    "args": cloud_run_job_dict["template"]["template"]["containers"][0].get("args", []),
                    "max_retries": cloud_run_job_dict["template"]["template"]["max_retries"],
                    "timeout_seconds": int(cloud_run_job_dict["template"]["template"]["timeout"].replace("s", "")),
                    "service_account": cloud_run_job_dict["template"]["template"]["service_account"]
                }

                desired_config = {
                    "task_count": config["task_count"],
                    "image": config["image"],
                    "command": config["command"],
                    "args": config["args"],
                    "max_retries": config["max_retries"],
                    "timeout_seconds": config["timeout_seconds"],
                    "service_account": config["service_account"]
                }

                if deployed_config != desired_config:
                    logging.info(f"Updating Cloud Run job: {job_name}.")
                    update_cloud_run_job(
                        project_id=project_id,
                        region=region,
                        job_name=job_name,
                        job_payload=job_payload
                    )


if __name__ == '__main__':
    main()

