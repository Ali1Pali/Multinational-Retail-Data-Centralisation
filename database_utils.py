from sqlalchemy import inspect
from sqlalchemy import create_engine
import pandas as pd
import yaml


class DatabaseConnector:
    def __init__(self, file_path: str):
        self.file_path = file_path

    #Read credentials from yaml file
    def read_db_creds(self):
        with open(self.file_path, 'r') as f:
            try:
                return yaml.safe_load(f)
            except yaml.YAMLError as err:
                print(err)
        
    #Initialise and return database engine
    def init_db_engine(self):
        creds = self.read_db_creds()
        db_url = f"{'postgresql'}+{'psycopg2'}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine
    
    #List available tables
    def list_db_tables(self):
        inspector = inspect(self.init_db_engine())
        return inspector.get_table_names()
    
    #Upload the Pandas Dataframe to database
    def upload_to_db(self, dataframe: pd.DataFrame, table_name: str):
        local_creds = self.read_db_creds()
        local_db_url = f"{'postgresql'}+{'psycopg2'}://{local_creds['LOCAL_USER']}:{local_creds['LOCAL_PASSWORD']}@{local_creds['LOCAL_HOST']}:{local_creds['LOCAL_PORT']}/{local_creds['LOCAL_DATABASE']}"
        engine = create_engine(local_db_url)
        engine.connect()
        dataframe.to_sql(name= table_name, con=engine, if_exists='replace', index=False)