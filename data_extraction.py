import pandas as pd
import requests
import tabula


class DataExtractor:

    def read_rds_table(self, db_conn, table_name: str):
        #Initialize database engine and check if table is included
        engine = db_conn.init_db_engine()
        tables = db_conn.list_db_tables()
        if table_name not in tables:
            raise ValueError(f"'{table_name}' is not included in this database")
        
        #Fetch specified table
        df = pd.read_sql_table(table_name, engine)
        return df
    
    def retrieve_pdf_data(self, link):
        pdf_list = tabula.read_pdf(link, pages='all')
        pdf_df = pd.concat(pdf_list)
        return pdf_df

    def list_number_of_stores(self, no_stores_endpoint, header_dict):
        response = requests.get(no_stores_endpoint, headers=header_dict)
        no_of_stores = response.json()
        return no_of_stores
    
    def retrieve_stores_data(self, retrieve_store_endpoint, header_dict):
        stores_list = []
        for store_no in range(0, 451):
            id_url = f'{retrieve_store_endpoint}/{store_no}'
            response = requests.get(id_url, headers=header_dict)
            stores_list.append(response.json())
        return pd.DataFrame(stores_list)
    
    def extract_from_s3(self, url):
       products = pd.read_csv(url)
       return products
    
    def retrieve_json(self, url):
        response = requests.get(url)
        json_data = response.json()
        date_events_data = pd.DataFrame.from_dict(json_data)
        return date_events_data