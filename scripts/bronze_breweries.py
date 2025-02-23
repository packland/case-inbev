import requests
import json
from pathlib import Path
from config import Settings
from datetime import datetime

class FetchBreweries:
    def __init__(self) -> None:
        settings = Settings()
        self.url = settings.FETCH_URL
        self.data_lake_dir = settings.DATA_LAKE_DIR

    def fetch_data(self) -> list[dict]:
        response = requests.get(self.url)
        response.raise_for_status()
        return response.json()

    def save_data(self, data: list[dict], file_path: str) -> None:
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w') as f:
            json.dump(data, f)

    def execute(self) -> None:
        timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        data = self.fetch_data()
        file_path = Path(self.data_lake_dir) / 'bronze' / f'{timestamp}_breweries.json'
        print(file_path)
        self.save_data(data, file_path)