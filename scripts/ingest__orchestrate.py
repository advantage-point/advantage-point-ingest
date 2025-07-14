from scripts.update_bigquery import main as update_bigquery
from scripts.update_cloud_run import main as update_cloud_run
from scripts.update_cloud_scheduler import main as update_cloud_scheduler
import logging

def main():

    # set logging config
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    )

    # update bigquery
    update_bigquery()

    # update cloud run
    update_cloud_run()

    # handle cloud scheduler
    update_cloud_scheduler()

if __name__ == '__main__':
    main()