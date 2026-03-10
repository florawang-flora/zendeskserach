from zendeskserach.utils.load_config import load_conf
from zendeskserach.src.ingestion.ingestion_1 import Ingestion
# test load config success
def test_load_config_success():
    # yaml file return dict output
    # check the output whether is a dictionary output.
    config = load_conf()
    assert isinstance(config, dict)
    # check whether we're missing key
    assert "base_path" in config
    #assert "base_path_wrong" in config



def test_ingestion_parameter():
    config = load_conf()
    ingestion1 = Ingestion(
        config["base_path"],
        config['data_source'],
        config['compulsory_rules'],
        config['schema_rules']
    )

    assert ingestion1._base_path == config["base_path"]
    assert ingestion1._data_source == config["data_source"]
    assert ingestion1._compulsory_rules == config['compulsory_rules']
    #assert ingestion1._schema_rules == config['schema_rules ']

def test_ingestion_read_files():
    config = load_conf()
    ingestion = Ingestion(
        config["base_path"],
        config['data_source'],
        config['compulsory_rules'],
        config['schema_rules']
    )
    data = ingestion._load_all_json(
    )
    assert isinstance(data, dict)

