from datetime import datetime
from scripts.web.tennisabstract.players.get_player_data import get_player_data
from scripts.web.tennisabstract.players.get_player_url_list import get_player_url_list
from utils.bigquery.alter_target_table import alter_target_table
from utils.bigquery.check_table_existence import check_table_existence
from utils.bigquery.create_table_with_cloud_storage import create_table_with_cloud_storage
from utils.bigquery.create_target_table import create_target_table_with_cloud_storage
from utils.bigquery.drop_table import drop_table
from utils.bigquery.insert_target_table import insert_target_table
from utils.bigquery.get_control_object_record_full import get_control_object_record_full
from utils.bigquery.update_target_table import update_target_table
from utils.cloud_storage.delete_cloud_storage_objects import delete_cloud_storage_objects
from utils.cloud_storage.get_cloud_storage_objects import get_cloud_storage_objects
from utils.cloud_storage.write_batch_to_cloud_storage import write_batch_to_cloud_storage
import logging

def main():

    # set logging config
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    )

    try:

        today_str = datetime.now().strftime("%Y%m%d")

        # query for control table record
        bigquery_target_table_id = 'raw__web__tennisabstract__players'
        table_record_dict = get_control_object_record_full(
            target_table_id=bigquery_target_table_id
        )

        # get player url list
        player_url_list = get_player_url_list()[:20]
        player_url_list_len = len(player_url_list)

        # parse control table record
        bigquery_target_dataset_id = table_record_dict['bigquery_target_dataset_id']
        bigquery_target_dataset_location = table_record_dict['bigquery_target_dataset_location']
        bigquery_target_project_id = table_record_dict['bigquery_target_project_id']
        bigquery_target_table_id = table_record_dict['bigquery_target_table_id']
        bigquery_temp_dataset_id = table_record_dict['bigquery_temp_dataset_id']
        bigquery_temp_dataset_location = table_record_dict['bigquery_temp_dataset_location']
        bigquery_temp_project_id = table_record_dict['bigquery_temp_project_id']
        bigquery_temp_table_id = table_record_dict['bigquery_temp_table_id']
        cloudstorage_bucket_name = table_record_dict['cloudstorage_bucket_name']
        cloudstorage_folder_name_prefix = table_record_dict['cloudstorage_folder_name_prefix']
        cloudstorage_object_name_prefix = table_record_dict['cloudstorage_object_name_prefix']
        source_load_record_batch_count = table_record_dict['source_load_record_batch_count'] or player_url_list_len
        unique_column_name_list = table_record_dict['unique_column_name_list']

        # create cloud storage properties
        cloudstorage_folder_name = f"{cloudstorage_folder_name_prefix}/{today_str}"
        cloudstorage_to_bigquery_object_pattern = f"{cloudstorage_folder_name}/*.json"

        # delete cloud storage objects (if exist from previous run)
        delete_cloud_storage_objects(
            bucket_name=cloudstorage_bucket_name,
            prefix=f"{cloudstorage_folder_name}/"
        )

        # loop through records
        for i in range(0, player_url_list_len, source_load_record_batch_count):

            # process the current batch
            batch_number = i // source_load_record_batch_count + 1
            batch_number_fmt = f"{batch_number:06d}"
            start_idx = i
            end_idx = min(i + source_load_record_batch_count, player_url_list_len)
            player_url_list_batch = player_url_list[start_idx:end_idx]

            logging.info(f"Processing records {start_idx} to {end_idx - 1} (batch size: {len(player_url_list_batch)}).")

            # get player data
            player_data_list = get_player_data(
                player_url_list=player_url_list_batch
            )

            if player_data_list:

                # upload to cloud storage
                cloudstorage_object_name = f"{cloudstorage_object_name_prefix}__{today_str}__{batch_number_fmt}.json"
                cloudstorage_object_path = f"{cloudstorage_folder_name}/{cloudstorage_object_name}"
                write_batch_to_cloud_storage(
                    record_list=player_data_list,
                    bucket_name=cloudstorage_bucket_name,
                    object_path=cloudstorage_object_path
                )

        # check cloud storage file(s) existence
        cloud_storage_objects_list = get_cloud_storage_objects(
            bucket_name=cloudstorage_bucket_name,
            prefix=f"{cloudstorage_folder_name}/"
        )
        cloud_storage_objects_exists_flag = len(cloud_storage_objects_list) > 0

        if cloud_storage_objects_exists_flag == True:

            # check target table existence
            target_table_exists_flag = check_table_existence(
                project_id=bigquery_target_project_id,
                dataset_id=bigquery_target_dataset_id,
                table_id=bigquery_target_table_id
            )

            if target_table_exists_flag == True:

                # drop temp table
                drop_table(
                    project_id=bigquery_temp_project_id,
                    dataset_id=bigquery_temp_dataset_id,
                    table_id=bigquery_temp_table_id
                )

                # create temp table
                create_table_with_cloud_storage(
                    cloudstorage_bucket_name=cloudstorage_bucket_name,
                    cloudstorage_object_pattern=cloudstorage_to_bigquery_object_pattern,
                    bigquery_project_id=bigquery_temp_project_id,
                    bigquery_dataset_id=bigquery_temp_dataset_id,
                    bigquery_dataset_location=bigquery_temp_dataset_location,
                    bigquery_table_id=bigquery_temp_table_id
                )
                
                # alter target table
                alter_target_table(
                    target_project_id=bigquery_target_project_id,
                    target_dataset_id=bigquery_target_dataset_id,
                    target_table_id=bigquery_target_table_id,
                    source_project_id=bigquery_temp_project_id,
                    source_dataset_id=bigquery_temp_dataset_id,
                    source_table_id=bigquery_temp_table_id,
                    unique_column_name_list=unique_column_name_list
                )

                # update target table
                update_target_table(
                    target_project_id=bigquery_target_project_id,
                    target_dataset_id=bigquery_target_dataset_id,
                    target_table_id=bigquery_target_table_id,
                    source_project_id=bigquery_temp_project_id,
                    source_dataset_id=bigquery_temp_dataset_id,
                    source_table_id=bigquery_temp_table_id,
                    unique_column_name_list=unique_column_name_list
                )

                # insert
                insert_target_table(
                    target_project_id=bigquery_target_project_id,
                    target_dataset_id=bigquery_target_dataset_id,
                    target_table_id=bigquery_target_table_id,
                    source_project_id=bigquery_temp_project_id,
                    source_dataset_id=bigquery_temp_dataset_id,
                    source_table_id=bigquery_temp_table_id,
                    unique_column_name_list=unique_column_name_list
                )

                # drop temp table
                drop_table(
                    project_id=bigquery_temp_project_id,
                    dataset_id=bigquery_temp_dataset_id,
                    table_id=bigquery_temp_table_id
                )
            
            else:
                # otherwise create/load target table
                create_target_table_with_cloud_storage(
                    cloudstorage_bucket_name=cloudstorage_bucket_name,
                    cloudstorage_object_pattern=cloudstorage_to_bigquery_object_pattern,
                    bigquery_project_id=bigquery_target_project_id,
                    bigquery_dataset_id=bigquery_target_dataset_id,
                    bigquery_dataset_location=bigquery_target_dataset_location,
                    bigquery_table_id=bigquery_target_table_id
                )
        
        else:
            logging.info(f"No data ingested into Cloud Storage.")

    except Exception as e:
        logging.error(f"Error with ingestion process for {bigquery_target_table_id}: {e}")

if __name__ == '__main__':
    main()