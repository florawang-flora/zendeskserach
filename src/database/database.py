from sqlalchemy import create_engine
import pandas as pd
import json
class SQLDatabase:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
    def make_sqlite_safe(self,df):
        df = df.copy()
        for col in df.columns:
            df[col] = df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
            )
        return df
    def write_table(self, table_name,df):
        df_clean = self.make_sqlite_safe(df)
        df_clean.to_sql(table_name, self.engine, if_exists='replace',index= False)
        print(f"table {table_name} have written in the database. {df_clean.shape}")
    def read_query(self, query):
        return pd.read_sql(query, self.engine)



