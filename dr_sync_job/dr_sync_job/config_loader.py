import json

def load_config(file_path: str) -> dict:
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config
