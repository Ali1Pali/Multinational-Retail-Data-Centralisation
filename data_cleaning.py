import pandas as pd
import numpy as np


class DataCleaning:

    def remove_null(self, dataframe: pd.DataFrame):
        # Remove null values and duplicates
        dataframe.replace("NULL", np.nan, inplace=True)
        dataframe.drop_duplicates(inplace=True)
        dataframe.dropna(inplace=True)
        return dataframe
    
    def clean_invalid_date(self, dataframe: pd.DataFrame, column_name):
        # Convert date columns to datetime
        dataframe[column_name] = pd.to_datetime(dataframe[column_name], errors= 'coerce', format= 'mixed')
        return dataframe

    def clean_user_data(self, user_data: pd.DataFrame):
        # Set index column
        user_data.drop(columns='index', inplace=True)
        
        # Remove null and duplicate values
        user_data = self.remove_null(user_data)
        
        # Correct date values
        user_data = self.clean_invalid_date(user_data, 'date_of_birth')
        user_data = self.clean_invalid_date(user_data, 'join_date')
        
        # Correct country codes
        user_data['country_code'].replace('GGB', 'GB', inplace=True)
        
        # Remove incorrect values
        user_data = user_data[user_data['country_code'].isin(['DE', 'GB', 'US'])]
        return user_data
    
    def clean_card_data(self, card_data: pd.DataFrame):
        # Correct card numbers
        card_data['card_number'] = pd.to_numeric(
            card_data['card_number'].astype(str).str.extract('(\d+)', expand=False), errors='coerce')

        # Correct date values
        card_data = self.clean_invalid_date(card_data, 'date_payment_confirmed')
        
        # Remove incorrect rows
        card_data.drop_duplicates(inplace=True)
        card_data.dropna(inplace=True)
        
        # Change column dtypes
        card_data['card_number'] = card_data['card_number'].astype('int64')
        card_data['card_provider'] = card_data['card_provider'].astype('category')

        # Reset index
        card_data = card_data.reset_index(drop=True)
        return card_data

    def clean_store_date(self, stores_list_data: pd.DataFrame):
        # Remove lat column
        stores_list_data.drop(columns='lat', inplace=True)

        # Correct null values in online store row
        stores_list_data.iloc[0] = stores_list_data.iloc[0].fillna('N/A')

        # Format staff numbers
        stores_list_data['staff_numbers'] = pd.to_numeric(
            stores_list_data['staff_numbers'].astype(str).str.extract('(\d+)', expand=False), errors='coerce')

        # Remove incorrect and duplicate rows
        stores_list_data = stores_list_data[stores_list_data['country_code'].isin(['DE', 'GB', 'US'])]
        stores_list_data.drop_duplicates(inplace=True)

        # Correct continent column
        continent_map = {'eeAmerica' : 'America', 'eeEurope' : 'Europe'}
        stores_list_data.replace({'continent': continent_map}, inplace=True)

        # Correct date values
        stores_list_data = self.clean_invalid_date(stores_list_data, 'opening_date')

        # Correct index column
        stores_list_data.drop(columns='index', inplace=True)
        return stores_list_data
    
    def convert_product_weights(self, products_data: pd.DataFrame):
        
        def convert_to_kg(weight):
            if isinstance(weight, float):
                return weight
            elif 'x' in weight:
                quantity = float(weight.split('x')[0].strip())
                weight = float(weight.split('x')[1].replace('g', '').strip())
                return (quantity * weight) / 1000
            elif weight.endswith('.'):
                return float(weight.split(' ')[0].replace('g', ''))
            elif 'ml' in weight:
                return float(weight.replace('ml', '')) / 1000
            elif 'kg' in weight:
                return float(weight.replace('kg', ''))
            elif 'g' in weight:
                return float(weight.replace('g', '')) / 1000
            else:
                return np.nan
        
        products_data['weight'] = products_data['weight'].apply(convert_to_kg)
        products_data['weight'] = pd.to_numeric(products_data['weight'], errors='coerce').round(3)
        return products_data
    
    def clean_products_data(self, products_data: pd.DataFrame):
        # Remove null data
        products_data = self.remove_null(products_data)

        # Remove incorrect rows
        products_data = products_data[products_data['category'].isin(['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 'food-and-drink', 'diy'])]

        # Correct dates values
        products_data = self.clean_invalid_date(products_data, 'date_added')

        # Change column dtypes
        products_data['product_price'] = products_data['product_price'].str.replace('£', '')
        products_data['product_price'] = products_data['product_price'].astype('float')
        products_data['category'] = products_data['category'].astype('category')
        products_data['removed'] = products_data['removed'].astype('category')

        # Correct index column
        products_data.drop('Unnamed: 0', axis=1, inplace=True)
        return products_data
    
    def clean_orders_data(self, order_data: pd.DataFrame):
        # Remove invalid columns and rows
        order_data.drop(['first_name', 'last_name', '1', 'level_0'], axis=1, inplace=True)
        order_data = self.remove_null(order_data)
        
        # Correct card numbers
        order_data['card_number'] = pd.to_numeric(
            order_data['card_number'].astype(str).str.extract('(\d+)', expand=False), errors='coerce')
        
        # Change column dtypes
        order_data['card_number'] = order_data['card_number'].astype('int64')

        # Correct index column
        order_data.drop(columns='index', inplace=True)
        return order_data
    
    def clean_date_events_data(self, date_events_data: pd.DataFrame):
        # Remove invalid rows
        date_events_data = self.remove_null(date_events_data)
        date_events_data = date_events_data[date_events_data['time_period'].isin(['Evening', 'Morning', 'Midday', 'Late_Hours'])]

        # Change column dtypes
        date_events_data['time_period'] = date_events_data['time_period'].astype('category')
        return date_events_data
