from kafka import KafkaProducer
import requests
import json
import time
import logging


class APIFetcher:

    '''
    Fetches and formats data from an API.

    Args:
        api_url (str): The URL of the API endpoint.

    Methods:
        get_data(self, element: str, index: int = 0) -> dict:
            Fetches data from the API and returns the specified element at the given index.
        
        _get_nested_value(self, dict: dict, keys: list) -> dict:
            Retrieves a nested value from a dictionary using a list of keys.
        
        format_data(self, data: dict, mapping: dict) -> dict:
            Formats data according to the specified mapping and returns the result.
    '''

    def __init__(self, api_url: str) -> None:
        '''Initialize APIFetcher with the given API URL.'''
        self.api_url = api_url
    
    def get_data(self, element: str, index: int = 0) -> dict:
        '''Fetch data from the API and return the specified element at the given index.'''
        try:
            response = requests.get(self.api_url).json()
            response = response[element][index]
            return response
        except Exception as e:
            logging.error(f'Could not fetch data from API: {e}')
            raise
    
    def _get_nested_value(self, dict: dict, keys: list) -> dict:
        '''Retrieve a nested value from a dictionary using a list of keys.'''
        for key in keys:
            dict = dict.get(key, {})
        return dict
    
    def format_data(self, data: dict, mapping: dict) -> dict:
        '''Format data according to the specified mapping and return the result.'''
        result = {}
        for key, value in mapping.items():
            if isinstance(value, list):
                result[key] = self._get_nested_value(data, value)
            elif isinstance(value, dict):
                result[key] = value.get('format', '{}').format(
                    **{k: self._get_nested_value(data, v) for k, v in value.get('values', {}).items()}
                )
            else:
                result[key] = data.get(value, '')
        return result
    

class KafkaStreamer(APIFetcher):

    '''
    Streams formatted data to a Kafka topic.

    Args:
        api_url (str): The URL of the API endpoint.
        bootstrap_servers (str): The Kafka bootstrap servers.

    Methods:
        connect_producer(self, max_block_ms: int = 5000) -> KafkaProducer:
            Creates and returns a KafkaProducer connected to the specified bootstrap servers.
        
        stream_data(self, producer: KafkaProducer, topic: str, data: dict, time_interval: int = 60, encode: str = 'utf-8') -> None:
            Streams data to the specified Kafka topic for a given time interval.
    '''
        
    def __init__(self, api_url: str, bootstrap_servers: str) -> None:
        '''Initialize KafkaStreamer with API URL and Kafka bootstrap servers.'''
        super().__init__(api_url)
        self.bootstrap = bootstrap_servers

    def connect_producer(self, max_block_ms: int = 5000) -> KafkaProducer:
        '''Create and return a KafkaProducer connected to the specified bootstrap servers.'''
        try:
            producer = KafkaProducer(bootstrap_servers=[self.bootstrap], max_block_ms=max_block_ms)
            logging.info('Kafka producer connected successfully')
            return producer
        except Exception as e:
            logging.error(f'Could not connect to Kafka server: {e}')
            raise

    def stream_data(self, producer: KafkaProducer, topic: str, data: dict, time_interval: int = 60, encode: str = 'utf-8') -> None:
        '''Stream data to the specified Kafka topic for a given time interval.'''
        curr_time = time.time()
        while time.time() <= curr_time + time_interval:
            try:
                producer.send(topic=topic, value=json.dumps(data).encode(encode))
                logging.info(f'Data sent to topic {topic}: {data}')
            except Exception as e:
                logging.error(f'An error occurred during streaming: {e}')
                time.sleep(3)  # Pause briefly before retrying to prevent rapid error logging
        producer.flush()
        producer.close()
        logging.info('Streaming finished and producer closed.')

