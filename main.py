from src.ingestion.ingestion_1 import Ingestion
from src.curation.curation_1 import Curation
from src.database.database import SQLDatabase
from utils.load_config import load_conf
import yaml
def main():
    config = load_conf()
    base_path = config['base_path']
    data_source = config['data_source']
    compulsory_rules = config['compulsory_rules']
    schema_rules = config['schema_rules']
    file_names = config['data_source']
    ingestion_data = Ingestion(base_path, data_source, compulsory_rules,schema_rules)
    # generate multipe dataframe
    dfs = ingestion_data.ingestion_run()

    curation = Curation(file_names,dfs)

    curation_dataset= curation.generate_database_format()
    #print(curation_dataset)
    db_url = config['database']['url']
    db = SQLDatabase(db_url)
    db.write_table('merge_datasets', curation_dataset)
    results = db.read_query("SELECT * FROM merge_datasets LIMIT 10")
    # print(results)
if __name__ == "__main__":
    main()
