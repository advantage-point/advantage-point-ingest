from scripts.web.tennisabstract.matches.get_match_data_df import get_match_data_df
from scripts.web.tennisabstract.matches.get_match_url_list import get_match_url_list
from utils.bigquery.alter_target_table import alter_target_table
from utils.bigquery.check_table_existence import check_table_existence
from utils.bigquery.create_table_with_df import create_table_with_df
from utils.bigquery.create_target_table import create_target_table
from utils.bigquery.drop_table import drop_table
from utils.bigquery.insert_target_table import insert_target_table
from utils.bigquery.get_control_object_record_full import get_control_object_record_full
from utils.bigquery.update_target_table import update_target_table
import argparse
import importlib
import logging

def main():

    # set logging config
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    )

    try:

        # query for control table record
        table_record_dict = get_control_object_record_full(
            target_table_id='raw__web__tennisabstract__matches'
        )

        # get match url list
        match_url_list = get_match_url_list()
        match_url_list_len = len(match_url_list)

        # parse control table record
        source_load_record_batch_count = table_record_dict['source_load_record_batch_count'] or match_url_list_len
        target_project_id = table_record_dict['bigquery_target_project_id']
        temp_project_id = table_record_dict['bigquery_temp_project_id']
        target_dataset_id = table_record_dict['bigquery_target_dataset_id']
        temp_dataset_id = table_record_dict['bigquery_temp_dataset_id']
        target_table_id = table_record_dict['bigquery_target_table_id']
        temp_table_id = table_record_dict['bigquery_temp_table_id']
        unique_column_name_list = table_record_dict['unique_column_name_list']

        for i in range(0, match_url_list_len, source_load_record_batch_count):

            # process the current batch
            logging.info(f"Processing batch {i // source_load_record_batch_count + 1}: {match_url_list_len} records.")
            match_url_list_batch = match_url_list[i:i + source_load_record_batch_count]

            # create dataframe
            match_data_df = get_match_data_df(
                match_url_list=match_url_list_batch
            )

            # drop temp table
            drop_table(
                project_id=temp_project_id,
                dataset_id=temp_dataset_id,
                table_id=temp_table_id
            )

            # create temp table
            create_table_with_df(
                project_id=temp_project_id,
                dataset_id=temp_dataset_id,
                table_id=temp_table_id,
                dataframe=match_data_df
            )

            # check target table existence
            target_table_exists_flag = check_table_existence(
                project_id=target_project_id,
                dataset_id=target_dataset_id,
                table_id=target_table_id
            )

            if target_table_exists_flag == True:
                
                # alter target table
                alter_target_table(
                    target_project_id=target_project_id,
                    target_dataset_id=target_dataset_id,
                    target_table_id=target_table_id,
                    source_project_id=temp_project_id,
                    source_dataset_id=temp_dataset_id,
                    source_table_id=temp_table_id,
                    unique_column_name_list=unique_column_name_list
                )

                # update target table
                update_target_table(
                    target_project_id=target_project_id,
                    target_dataset_id=target_dataset_id,
                    target_table_id=target_table_id,
                    source_project_id=temp_project_id,
                    source_dataset_id=temp_dataset_id,
                    source_table_id=temp_table_id,
                    unique_column_name_list=unique_column_name_list
                )

                # insert
                insert_target_table(
                    target_project_id=target_project_id,
                    target_dataset_id=target_dataset_id,
                    target_table_id=target_table_id,
                    source_project_id=temp_project_id,
                    source_dataset_id=temp_dataset_id,
                    source_table_id=temp_table_id,
                    unique_column_name_list=unique_column_name_list
                )
            
            else:
                # otherwise create/load target table
                create_target_table(
                    project_id=target_project_id,
                    dataset_id=target_dataset_id,
                    table_id=target_table_id,
                    dataframe=match_data_df
                )

            # drop temp table
            drop_table(
                project_id=temp_project_id,
                dataset_id=temp_dataset_id,
                table_id=temp_table_id
            )

    except Exception as e:
        logging.error(f"Error with ingestion process for {target_table_id}: {e}")

if __name__ == '__main__':
    main()