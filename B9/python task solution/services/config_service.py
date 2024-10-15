import json
from models.config import Config

class ConfigService:
    def read_config(file_path: str) -> Config:
        with open(file_path, 'r') as file:
            config_dict = json.load(file)

        return Config(**config_dict)