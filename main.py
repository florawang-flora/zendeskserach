from src.ingestion.ingestion_1 import Ingestion
from src.curation.curation_1 import Curation
from src.database.database import SQLDatabase
import yaml
def main():
    config_path = "/Users/mac/PycharmProjects/Zendesk_research/config.yml"
    with open(config_path,'r') as file:
        config = yaml.safe_load(file)
    print(config)
    ingestion_data = Ingestion(config_path='config.yml')
    # generate multipe dataframe
    dfs = ingestion_data.ingestion_run()
    curation = Curation(dfs)
    curation_dataset= curation.generate_database_format()
    #print(curation_dataset)
    db_url = config['database']['url']
    db = SQLDatabase(db_url)
    db.write_table('merge_datasets', curation_dataset)
    results = db.read_query("SELECT * FROM merge_datasets LIMIT 10")
    # print(results)
if __name__ == "__main__":
    main()
