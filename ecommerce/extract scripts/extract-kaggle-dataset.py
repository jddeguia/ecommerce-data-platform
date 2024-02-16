import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import duckdb

class KaggleDuckDBHandler:
    def __init__(self, kaggle_username, kaggle_key):
        self.api = KaggleApi()
        self.api.authenticate(username=kaggle_username, key=kaggle_key)
    def download_csv_from_kaggle(self, dataset_or_url, file_name):
        self.api.dataset_download_files(dataset_or_url, path='.', unzip=True)
    def read_csv_to_dataframe(self, file_path):
        return pd.read_csv(file_path)
    def write_dataframe_to_duckdb_table(self, df, table_name, db_path):
        con = duckdb.connect(db_path)
        con.register('df', df)
        con.execute(f'CREATE TABLE {table_name} AS SELECT * FROM df')

def main():
    kaggle_username = "your-kaggle-username"
    kaggle_key = "your-kaggle-api-key"
    dataset_url = "https://www.kaggle.com/your-username/your-dataset-name"
    file_name = "your-file-name.csv"
    db_path = "your-db-path.db"
    table_name = "your-table-name"
    kaggle_duckdb_handler = KaggleDuckDBHandler(kaggle_username, kaggle_key)
    kaggle_duckdb_handler.download_csv_from_kaggle(dataset_url, file_name)
    df = kaggle_duckdb_handler.read_csv_to_dataframe(file_name)
    kaggle_duckdb_handler.write_dataframe_to_duckdb_table(df, table_name, db_path)

if __name__ == "__main__":
    main()