import yaml
import logging
from pyspark.sql.types import StructType, StructField, StringType


class ConfigParser:
    
    '''
    Handles configuration file operations for API and database details.

    Args:
        config_file (str): Path to the YAML configuration file.

    Methods:
        _load_config() -> dict:
            Loads and returns the configuration file content as a dictionary.

        get_api_info(api: str, info: str) -> str:
            Retrieves specific API information from the configuration file.

        get_db_info(db: str, table_name: str, info: str):
            Retrieves specific database information, such as schema or column names, from the configuration file.

        get_column_placeholders(db: str, table_name: str) -> str:
            Generates a string of column placeholders for the specified table.

        get_values_placeholders(db: str, table_name: str) -> str:
            Generates a string of placeholders for values for the specified table.

        get_column_values(db: str, table_name: str, **kwargs) -> list:
            Retrieves column values from kwargs based on the table schema.
    '''

    def __init__(self, config_file: str) -> None:
        '''Initialize ConfigParser with the path to the configuration file and load the configuration.'''
        self.config_file = config_file
        self.config = self._load_config()

    def _load_config(self) -> dict:
        '''Load the configuration file and return its content as a dictionary.'''
        try:
            with open(self.config_file, 'r') as file:
                config = yaml.safe_load(file)
            return config
        except Exception as e:
            logging.error(f"Failed to load config file {self.config_file}: {e}")
            raise

    def get_api_info(self, api: str, info: str) -> str:
        '''Retrieve specific API information from the configuration file.'''
        return self.config['api'][api][info]

    def get_db_info(self, db: str, table_name: str, info: str):
        '''Retrieve specific database information, such as schema or column names, from the configuration file.'''
        if info == 'schema':
            schema = StructType()
            for field in self.config['databases'][db][table_name]['schema']:
                schema.add(field['column_name'], eval(field['data_type'])(), field['nullable'])
            return schema
        elif info == 'column_names':
            return [field['column_name'] for field in self.config['databases'][db][table_name]['schema']]
        else:
            return self.config['databases'][db][table_name][info]

    def get_column_placeholders(self, db: str, table_name: str) -> str:
        '''Generate a string of column placeholders for the specified table.'''
        column_names = self.get_db_info(db, table_name, 'column_names')
        return ', '.join(column_names)

    def get_values_placeholders(self, db: str, table_name: str) -> str:
        '''Generate a string of placeholders for values for the specified table.'''
        column_names = self.get_db_info(db, table_name, 'column_names')
        return ', '.join(['%s'] * len(column_names))

    def get_column_values(self, db: str, table_name: str, **kwargs) -> list:
        '''Retrieve column values from kwargs based on the table schema.'''
        column_names = self.get_db_info(db, table_name, 'column_names')
        return [kwargs.get(column) for column in column_names]