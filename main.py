print('file start running ')
from ingestion.load_json import load_json
from ingestion.validate_json import validate_records_simple
from ingestion.clean_json import clean_json
from ingestion.generate_dataframe import generate_dataframe

def run_pipeline():
    # load the data
    data = load_json()
    print("Loaded keys:", data.keys())

    # for each json files required field (validation setup)
    compulsory_rules = {
        "tickets.json": ["_id" ,"url","external_id","submitter_id", "assignee_id","organization_id"],
        "users.json": ["_id",  "external_id", "organization_id" ],
        "organizations.json": ["_id", "external_id"]
    }
    # data {'organizations.json': [{'_id': 101, 'url': 'http:/..'}] , users : }


    # clean  setup
    schema_rules = {
        "tickets.json": ["_id" ,"url","external_id","created_at", "type", "subject", "description",
                         "priority", "status", "submitter_id", "assignee_id", "organization_id",
                         "tags", "has_incidents", "due_at" ,"via" ],
        "users.json": ["_id", "external_id", "name", "alias","created_at", "active" ,"verified",
                       "shared", "locale", "timezone", "last_login_at" ,"email", "phone", "signature",
                       "organization_id", "tags", "suspended", "role"],

        "organizations.json": ["_id" ,"url","external_id","name", "domain_name","created_at", "details",
            "shared_tickets", "tags"]
    }
    for file_name, records in data.items():
        # validating the data.
        #print(f'here is the record example {records}')
        required_fields = compulsory_rules[file_name]
        valid_rec, errors = validate_records_simple(
            records=records,
            required_fields=required_fields,
            file_name=file_name
        )
        #print("vvvvvv",valid_rec)
        print(f"{file_name}: valid = {len(valid_rec)}, errors = {len(errors)}")

        if len(errors) != 0:
            print(f'Here is the error information:{errors}, please go to the raw data source')

        # cleaning data
        print('Cleaning records...')
        schema_fields = schema_rules[file_name]
        clean_records = clean_json(valid_rec, schema_fields)
        #print(clean_records)
        print(f"Cleaning records Finished ：{len(clean_records)} record has been cleaning")

        # generate dataframe

        print("Generating dataframe...")
        df = generate_dataframe(clean_records)
        print(f"{file_name} dataframe has been generate out")
        print(df.head())
        print(f"{df.shape[0]} rows,{df.shape[1]} columns")

        # change list of dict to dataframe format

if __name__ == '__main__':
    run_pipeline()




