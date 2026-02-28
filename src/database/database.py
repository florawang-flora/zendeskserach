from sqlalchemy import create_engine,text
import pandas as pd
import json
class SQLDatabase:
    def __init__(self, db_url, clean_datasets):
        self.engine = create_engine(db_url)
        self._clean_dfs = clean_datasets
    # step1 : create 3 base tables
    def generate_database_data(self, clean_datasets):
        for file_name in clean_datasets:
            self.write_table(file_name,clean_datasets[file_name])

    def _make_sqlite_safe(self,df):
        # change the tag field in the df into json format, otherwise can't generate sql format
        df = df.copy()
        for col in df.columns:
            df[col] = df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
            )
        return df
    def write_table(self, table_name,df):
        df_clean = self._make_sqlite_safe(df)
        df_clean.to_sql(table_name, self.engine, if_exists='replace',index= False)
        print(f"table {table_name} have written in the database. {df_clean.shape}")

    # second function is to read the query and to
    def read_query(self, query):
        return pd.read_sql(query, self.engine)


    ## step2: prepare 3 tables for the cli table
    def _search_organization_table(self):
        return '''
        DROP TABLE IF EXISTS organization_cli;
        CREATE TABLE organization_cli AS
        SELECT 
            o.*, 
            u.*,
            t.*
        FROM organizations AS o
        LEFT JOIN users AS u
            ON u.users_id = o.organization_id
        LEFT JOIN tickets AS t
            ON t.tickets_submitter_id = u.users_id;
        '''


    def _search_ticket_table(self):
        return '''
        DROP TABLE IF EXISTS ticket_cli;
        CREATE TABLE ticket_cli AS 
        SELECT 
            t.*, 
            u.*, 
            o.*
        FROM tickets as t
        LEFT JOIN users AS u
            ON t.tickets_submitter_id = u.users_id
        LEFT JOIN organizations as o
            ON o.organization_id = u.users_id ;
        '''


    def _search_user_table(self):
        return '''
        DROP TABLE IF EXISTS user_cli;
        CREATE TABLE user_cli AS 
        SELECT 
            u.*,
            t.*,
            o.*
        FROM users AS u 
        LEFT JOIN tickets AS t
            ON u.users_id = t.tickets_submitter_id 
        LEFT JOIN organizations AS o 
            ON o.organization_id = u.users_id;
        '''
        return user_sql
    def _execute_sql_script(self,sql):
        with self.engine.begin() as conn:
            for execute_line in sql.split(';'):
                execute_line = execute_line.strip()
                # get rid of empty line
                if execute_line:
                    conn.execute(text(execute_line))

    def generate_cli_datasets(self):
        self._execute_sql_script(self._search_organization_table())
        self._execute_sql_script(self._search_ticket_table())
        self._execute_sql_script(self._search_user_table())
        print("CLI tables have been successfully generated!")



