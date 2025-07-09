from scripts.create_datasets import create_datasets
import logging

def main():

    # set logging config
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    )

    # create datasets
    create_datasets()

if __name__ == '__main__':
    main()