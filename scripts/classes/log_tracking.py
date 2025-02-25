import csv
import os
from datetime import datetime
from config import Settings

class LogTracking:
    def __init__(self):
        settings = Settings()
        self.log_file_path = os.path.join(settings.DATA_LAKE_DIR, 'silver', 'execution_log.csv')
        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
        # Create the file if it doesn't exist
        if not os.path.exists(self.log_file_path):
            with open(self.log_file_path, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['data_de_execucao', 'nome_do_arquivo'])

    def return_last_execution(self):
        try:
            with open(self.log_file_path, mode='r') as file:
                reader = csv.DictReader(file)
                rows = list(reader)
                if rows:
                    return rows[-1]
                else:
                    return None
        except FileNotFoundError:
            return None

    def add_new_execution(self, nome_do_arquivo):
        with open(self.log_file_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            data_de_execucao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow([data_de_execucao, nome_do_arquivo])