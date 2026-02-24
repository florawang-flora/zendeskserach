from utils.load_config import load_conf
from src.ingestion.ingestion_1 import Ingestion

config = load_conf()

base_path = config['base_path']
data_source = config['data_source']
compulsory_rules = config['compulsory_rules']
schema_rules = config['schema_rules']


ingestion = Ingestion(base_path,data_source, compulsory_rules, schema_rules)


def test_demo():
    result = 5
    assert result == 5
def test_ingestions_result():
    config = load_conf()
    result = ingestion.ingestion_run()
    assert isinstance(result, dict)
