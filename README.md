# Multinational Retail Data Centralisation

## Overview

This project's aim is to extarct and clean data from several data sources, then add them to a local database, before using this database to run several queries and retrieve up-to-data metrics in order to make more data-driven decisions.

## Installation

1. Clone the repository: `git clone [repository-url]`
2. Install required dependencies: `pip install -r requirements.txt`
3. Set up your database credentials (external/local) in a YAML file e.g. `db_creds.yaml`

## Usage

 An example of the full code in use is included in the file `main.py`
 
 - Use the methods from the DataExtractor class in the `data_extraction.py` file to extract the data

 - Use the methods from the DataCleaning class in the `data_cleaning.py` file to clean the extracted data

  - Use the methods from the DatabaseConnector class in the `database_utils.py` file to extract your credentials then upload the data to a local database


## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.