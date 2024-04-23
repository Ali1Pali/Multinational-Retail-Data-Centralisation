from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

# Defining classes
db = DatabaseConnector('D:\AICore\sales_data\db_creds.yaml')
de = DataExtractor()
dc = DataCleaning()

# Get list of tables from online database
tables_list = db.list_db_tables()
print(tables_list)

# Extracting and cleaning user data
user_data = de.read_rds_table(db, 'legacy_users')   
user_data_clean = dc.clean_user_data(user_data)

# Uploading cleaned user data to local 'Sales_Data' database
db.upload_to_db(user_data_clean, 'dim_users')

# Extracting and cleaning card data
card_data = de.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
card_data_clean = dc.clean_card_data(card_data)

# Uploading cleaned card data to local 'Sales_Data' database
db.upload_to_db(card_data_clean, 'dim_card_details')

# Extracting number of stores
de.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})

# Extracting and cleaning store data
store_data = de.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details', {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})
store_data_clean = dc.clean_store_date(store_data)

# Uploading cleaned store data to local 'Sales_Data' database
db.upload_to_db(store_data_clean, 'dim_store_details')

# Extracting products data from s3 bucket
products_data = de.extract_from_s3('s3://data-handling-public/products.csv')

# Converting weight column to kg and cleaning product data
products_data_kg = dc.convert_product_weights(products_data)
products_data_clean = dc.clean_products_data(products_data_kg)

# Uploading cleaned products data to local 'Sales_Data' database
db.upload_to_db(products_data_clean, 'dim_products')

# Extracting and cleaning orders data
order_data = de.read_rds_table(db, 'orders_table')   
order_data_clean = dc.clean_orders_data(order_data)

# Uploading cleaned order data to local 'Sales_Data' database
db.upload_to_db(order_data_clean, 'orders_table')

# Extract date events data from json
date_events_data = de.retrieve_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

# Cleaning date events data
clean_date_events = dc.clean_date_events_data(date_events_data)

# Uploading cleaned date events data to local 'Sales_Data' database
db.upload_to_db(clean_date_events, 'dim_date_times')