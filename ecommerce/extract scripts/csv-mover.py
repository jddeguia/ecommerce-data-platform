import os
import shutil

class CsvFileCopier:
    def __init__(self, source_dir, dest_dir):
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        
        # Check if the source directory exists
        if not os.path.exists(self.source_dir):
            raise FileNotFoundError(f"Source directory '{self.source_dir}' does not exist.")
        
        # Check if the destination directory exists, if not create it
        if not os.path.exists(self.dest_dir):
            os.makedirs(self.dest_dir)
            print(f"Destination directory '{self.dest_dir}' created.")
    
    def copy_csv_files(self):
        # Look for CSV files in the source directory
        csv_files = [file for file in os.listdir(self.source_dir) if file.endswith('.csv')]
        
        # Check if there are any CSV files in the source directory
        if not csv_files:
            print(f"No CSV files found in '{self.source_dir}'.")
            return
        
        # Copy each CSV file to the destination directory
        for csv_file in csv_files:
            source_path = os.path.join(self.source_dir, csv_file)
            dest_path = os.path.join(self.dest_dir, csv_file)
            shutil.copy(source_path, dest_path)
            print(f"File '{csv_file}' copied from '{self.source_dir}' to '{self.dest_dir}'.")

    def rename_csv_files(self, new_filename):
        # Look for CSV files in the destination directory
        csv_files = [file for file in os.listdir(self.dest_dir) if file.endswith('.csv')]
        
        # Check if there are any CSV files in the destination directory
        if not csv_files:
            print(f"No CSV files found in '{self.dest_dir}'.")
            return
        
        # Rename each CSV file in the destination directory
        for csv_file in csv_files:
            old_path = os.path.join(self.dest_dir, csv_file)
            new_path = os.path.join(self.dest_dir, new_filename)
            os.rename(old_path, new_path)
            print(f"File '{csv_file}' renamed to '{new_filename}'.")

# Example usage:
source_directory = r"C:\Users\GL65\Desktop\Worker\ECommercePlatform\ecommerce-data-platform\ecommerce"
destination_directory = r"C:\Users\GL65\Desktop\Worker\ECommercePlatform\ecommerce-data-platform\ecommerce\csv to table"

copier = CsvFileCopier(source_directory, destination_directory)
copier.copy_csv_files()
copier.rename_csv_files("ecommerce.csv")
