import json
from pathlib import Path
def load_config(path: str = "config.json") -> dict:
    config_path = Path(path)
    
    if not config_path.exists():
        raise FileNotFoundError(f'Config file not found in {path}')
    
    with open(config_path, "r", encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f'JSON not valid {path}: {e}')
    
    return config