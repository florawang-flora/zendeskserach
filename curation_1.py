import pandas as pd
from ingestion_1 import Ingestion
class Curation:
# this part will make sure ,the task will do when we check the dataframe, it also will generate out another dataframe information:
# also at the same time
# 3 DATA FILE GENREATE TO 1 SQL
    def __init__(self,dict_of_dataframe):
        self.dict_of_dataframe = dict_of_dataframe
        self.file_name  = ['organizations.json', 'users.json','tickets.json']
        self.df_organization = dict_of_dataframe['organizations.json']
        self.df_users = dict_of_dataframe['users.json']
        self.df_tickets = dict_of_dataframe['tickets.json']

    def check_dataframes_format(self):
        # step1: check the dataframe format,
        # step2: check whether we have this dataframe.

        # step3: check  whether the primary key is unique/null, # business rule the _id should be unique.
        # we'll clean the dataframe after we load the data to the database.
        # check whether we have the dataframe:
        for file_name in self.file_name:
            dataframe = self.dict_of_dataframe[file_name]
            if dataframe is None :
                raise ValueError(f'Missing dataframe: {file_name}')
            if not isinstance(dataframe, pd.DataFrame):
                print(f'{self.file_name} is not a dataframe')

        # check the primary key value whether is unique.
        # will clean it after we load the data to database
        if self.df_users['_id'].isna().any():
            print('users _id has nulls')
        elif self.df_organization['_id'].isna().any():
            print('organization _id has nulls')
        elif self.df_tickets['_id'].isna().any():
            print('tickets _id has null')
        else:
            print("All primary key doesn't have NULL value")


        # check whether the primary key is unique:
        # just assume we don't have any dulplicates, otherwise I need to write another function.
        if not self.df_users['_id'].is_unique:
            raise ValueError("users _id has dulplicates ")
        elif not self.df_organization['_id'].is_unique:
            raise ValueError('organization _id has dulplicates')
        elif not self.df_tickets['_id'].is_unique:
            raise ValueError('tickets _id has dulplicates')
        else:
            print("All primary key is unique")
    def apply_business_logic(self):
        #https://pandas.pydata.org/docs/user_guide/merging.html#dataframe-join
        # rename the column name
        ###... I'll do it now .
        users_ticket = self.df_users.join(self.df_tickets)
        print(users_ticket.dtypes)





if __name__=="__main__":
    ingestion_data = Ingestion()
    # generate multipe dataframe
    dfs = ingestion_data.ingestion_run()
    print(type(dfs))
    curation_object = Curation(dfs)
    curation_log = curation_object.check_dataframes_format()
    curation_apply_business_logic = curation_object.apply_business_logic()








