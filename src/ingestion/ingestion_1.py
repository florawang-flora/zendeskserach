import json
import os
import pandas as pd
from utils.load_config import load_conf
class Ingestion:
    def __init__(self,base_path, data_source, compulsory_rules, schema_rules):
        self._base_path = base_path
        self._data_source = data_source
        self._compulsory_rules = compulsory_rules
        self._schema_rules = schema_rules
    @property
    def get_compulsory_rules(self):
        return self._compulsory_rules
    @property
    def get_schema_rules(self):
        return self._schema_rules

    def ingestion_run(self):
        print('===run start===')
        # return the dict file with all of 3 data sources
        #data={ticket.json: [{record1},{record2}],organization.json: [{record1},{record2}] }
        data = self._load_all_json()
        all_dataframes = {}
        for file_name, records in data.items():
            # print(f'Here is the file name {file_name}')
            #print(f'Here is the records {records}')
            required_fields = self._compulsory_rules
            # print(f'here is the required_fields: {required_fields}')
            valid_rec, errors = self._valid_records(
                records=records,
                required_fields=required_fields,
                file_name=file_name
            )
            clean_records = self._clean_json(valid_rec,file_name)
            #print(clean_records)
            df = self._generate_dataframe(clean_records,file_name)
            all_dataframes[file_name] = df
        print(all_dataframes)
        return all_dataframes
    # no meaning, private function and primary function.

    def _load_all_json(self):
        '''
        This function includes few steps:
        1) Find the file path, if the file doesn't exist, return the log information
        2) Load the json file. when we raise a error when we load the json file, will return error message. (JSONDecodeError)
        Return:
        # to define the output format, look at the input data format, think about what you want to represent as output.
        # all_data will return dict to record all of source file information
        # eg:all_data = {ticket.json: [{record1},{record2}],organization.json: [{record1},{record2}] }
        '''
        # file system check
        all_data = {}
        for file_name in self._data_source:
            file_path = os.path.join(self._base_path, file_name)
            print(f'Trying to load data file {file_path}')
            if not os.path.exists(file_path):
                print(f"We didn't load the file: {file_name}")
                all_data[file_name] = None
                continue
        # load the json file
            try:
                with open(file_path) as f:
                    all_data[file_name] = json.load(f)
                    print(f'Successfully load the file {file_name}')
            except json.JSONDecodeError:
                print(f"JSON format error : {file_name}")
                all_data[file_name] = None
                print("Loaded keys:", all_data.keys())
        return all_data
    def _valid_records(self,records, required_fields, file_name):
        '''
        This function is to mainly to check whether the json data field format is looks structure,
        if it doesn't, we take it out. In addition, we add the ticket record information in the error
        list. For the item that we take it out, it will pop into the missing list so that we can manaual
        clean it in the future.
        '''
        # records will be like =:  {'_id': 101, 'url': 'http:/..'}  which is a dict
        # return the clean data.
        clean = []
        # error means to create the error logging information
        errors = []

        # This is to check if there is no data in the json file, generate the log.
        if records is None:
            errors.append(f'{file_name}: records is None')
        # If the log format is not what we want, generate the warning.
        if not isinstance(records, list):
            errors.append(f"{file_name} records is not a list, got {type(records)} type")

        for i, line_rec in enumerate(records, start = 1):
            # start = 1 means i = 1 rather than 0
            # This is to check each item format, to help correct the data format in the future.
            if not isinstance(line_rec, dict):
                errors.append(f"{file_name}: is not dic type")
                continue
            # print(f"here is the data type:::::::{print(records)}")
            # we record data who has the missing information, to avoid interupt the pipeline, we store here to do the manual fix in the future.
            missing = []
            for field in required_fields[file_name]:
                # this is our rules, for example , if ticket.json lost external_id,
                # we plan to delete it this column, we plan to exclude this column
                # put it to the missing column, we can do the manual fix in the future.
                if field not in line_rec.keys():
                    missing.append(field)
            if missing:
                errors.append(f"{file_name}: row {i} missing {missing}")
            else:
               clean.append(line_rec)

        print(f"{file_name}: valid = {len(clean)}, errors = {len(errors)}")

        if len(errors) != 0:
            print(f'Here is the error information:{errors}, please go to the raw data source')

        return clean, errors

    def _clean_json(self,valid_records,file_name):
        """
           :param valid_records:list[dict], eg. [{record1}, {record2}]
           :param file_name: str,  eg. tickets.json
           :return: list[dict] , eg. [{clean_record1}, {clean_record2}]
           This function is to clean the json format inside and define the inside structure,
           also do the preparation for generating the dataframe.
           """
        schema_fields = self._schema_rules[file_name]
        new_dict = {}
        clean_list_records = []
        #print('here is the schema fields',schema_fields)
        print('Cleaning records...')
        # valid_records : [{dict_records}, {dict_records}, ]
        for dict_records in valid_records:
            for key, value in dict_records.items():
                if key not in schema_fields:
                    dict_records[key] = None

                if  isinstance(value, str):
                    value = value.strip()
            clean_list_records.append(dict_records)
        print(f"Cleaning records Finished ：{len(clean_list_records)} record has been cleaning in the {file_name}")
        print(f"clean_list_record data type is {type(clean_list_records)} in the {file_name}")
        return clean_list_records

    def _generate_dataframe(self, clean_list_records, file_name):
        print("Generating dataframe...")
        dataframe = pd.DataFrame(clean_list_records)
        dataframe.head()
        print(f"Have generated dataframe: { dataframe.shape[0]} rows,{dataframe.shape[1]} columns for the {file_name}")
        return dataframe














#if __name__ == '__main__':
#    config = load_conf()
#    base_path = config['base_path']
#    data_source = config['data_source']
#    compulsory_rules = config['compulsory_rules']
#    schema_rules = config['schema_rules']
#    file_names = config['data_source']
#
#
#
#
#    obj = Ingestion(base_path,data_source,compulsory_rules,schema_rules)
#    print(obj.get_compulsory_rules)
#    base_path ="/Users/mac/PycharmProjects/Zendesk_research/data_source/"
#    ingestion_data = Ingestion(base_path)
#    data = ingestion_data.get_load_all_json()
#
#
#    for file_name, records in data.items():
#        #print(f'Here is the file name {file_name}')
#        #print(f'Here is the records {records}')
#        required_fields = ingestion_data.compulsory_rules
#        #print(f'here is the required_fields: {required_fields}')
#
#
#        valid_rec, errors = ingestion_data.get_valid_records(
#            records=records,
#            required_fields=required_fields,
#            file_name=file_name
#        )
#
#        clean_records = ingestion_data.get_clean_json(valid_rec)
#
#        df = ingestion_data.get_generate_dataframe(clean_records)
#















