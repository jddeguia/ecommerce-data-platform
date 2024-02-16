import pandas as pd
import opendatasets as od

class KaggleDatasetHandler:
    def __init__(self):
        pass

    def download_csv_from_kaggle(self, dataset_url):
        od.download(dataset_url, force=True)


def main():
    dataset_url = "https://www.kaggle.com/datasets/aliessamali/ecommerce"
    kaggle_dataset_handler = KaggleDatasetHandler()
    kaggle_dataset_handler.download_csv_from_kaggle(dataset_url)
    
if __name__ == "__main__":
    main()
