from google.cloud import bigquery
# from utils.api__toast.get__orders import get__orders
from utils.bigquery.alter_target_table import alter_target_table
from utils.bigquery.check_table_existence import check_table_existence
from utils.bigquery.create_dataset import create_dataset
from utils.bigquery.create_table_with_df import create_table_with_df
from utils.bigquery.create_target_table import create_target_table
from utils.bigquery.drop_table import drop_table
from utils.bigquery.insert_target_table import insert_target_table
from utils.bigquery.query_control_object_record import query_control_object_record
from utils.bigquery.update_target_table import update_target_table
import argparse
import importlib
import logging

def main(
    target_table_id: str
):

    # set logging config
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    )

    # query for control table record
    table_record_dict = query_control_object_record(
        target_table_id=target_table_id
    )

    # parse control table record
    fetch_function_full_path = table_record_dict['fetch_function_full_path']
    target_project_id = table_record_dict['bigquery_target_project_id']
    temp_project_id = table_record_dict['bigquery_temp_project_id']
    target_dataset_id = table_record_dict['bigquery_target_dataset_id']
    temp_dataset_id = table_record_dict['bigquery_temp_dataset_id']
    target_table_id = table_record_dict['bigquery_target_table_id']
    temp_table_id = table_record_dict['bigquery_temp_table_id']
    unique_column_name_list = table_record_dict['unique_column_name_list']

    # get data
    fetch_function = importlib.import_module(fetch_function_full_path)
    data_df = fetch_function.main()
    if data_df is None or data_df.empty:
        logging.info("No data to process. Exiting.")
        return

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
        dataframe=data_df
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
        # otherwise create target table
        create_target_table(
            project_id=target_project_id,
            dataset_id=target_dataset_id,
            table_id=target_table_id,
            dataframe=data_df
        )

    # drop temp table
    drop_table(
        project_id=temp_project_id,
        dataset_id=temp_dataset_id,
        table_id=temp_table_id
    )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--target_table_id', required=True, help='Target table ID from control table')
    args = parser.parse_args()
    main(
        target_table_id=args.target_table_id
    )