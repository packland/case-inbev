import os
from dotenv import load_dotenv

class Settings:
    def __init__(self):
        load_dotenv(override=True)
        self.FETCH_URL = os.getenv('FETCH_URL')
        self.DATA_LAKE_DIR = os.getenv('DATA_LAKE_DIR')