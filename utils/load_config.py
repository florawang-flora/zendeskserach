import yaml
from pathlib import Path

def load_conf():
    current_file = Path(__file__)
    current_proejct_abs = Path(__file__).parent.parent.resolve()
    config_path = current_proejct_abs / 'config.yml'
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)
