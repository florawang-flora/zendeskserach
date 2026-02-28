import pandas as pd

from src.ingestion.ingestion_1 import Ingestion
from src.curation.curation_1 import Curation
from src.database.database import SQLDatabase
from utils.load_config import load_conf
import yaml
def main():
    # Configuration from the yaml file, located in the utils folder.
    config = load_conf()
    # take the variable out from the configuration file
    base_path = config['base_path']
    data_source = config['data_source']
    compulsory_rules = config['compulsory_rules']
    schema_rules = config['schema_rules']
    file_names = config['data_source']
    db_url = config['database']['url']
   # ingest data.
    ingestion_data = Ingestion(base_path, data_source, compulsory_rules,schema_rules)
    #  # eg. {ticket1.json : df1, organization1.json : df2 }
    dfs = ingestion_data.ingestion_run()

    # curate data
    # same data structure as ingestion_data, just apply the business logic which is rename the dataframe name.
    curation = Curation(file_names,dfs)
    curation_dataset= curation.generate_curation_dataset()

    # add the result to database and then generate 3 databases.
    db = SQLDatabase(db_url, curation_dataset)
    db.generate_database_data(curation_dataset)
    # generate  cli_dataframe
    db.generate_cli_datasets()




    results = db.read_query("SELECT * FROM organization_cli LIMIT 10")
    print(results)
if __name__ == "__main__":
    main()
