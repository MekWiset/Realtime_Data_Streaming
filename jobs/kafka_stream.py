import logging
from plugins.kafka_streamer import KafkaStreamer
from plugins.utils.configparser import ConfigParser


def kafka_stream(api_name: str, bootstrap_servers: str, topic: str) -> None:

    '''
    Fetches data from an API, formats it, and streams it to a Kafka topic.

    This function performs the following steps:
    1. Loads configuration from a YAML file using `ConfigParser`.
    2. Retrieves the API URL and data element configuration.
    3. Fetches raw data from the API using `KafkaStreamer`.
    4. Formats the raw data according to a mapping defined in the configuration.
    5. Connects to a Kafka producer.
    6. Streams the formatted data to the specified Kafka topic.
    '''

    config_path = 'config.yaml'
    conf = ConfigParser(config_file=config_path)

    api_url = conf.get_api_info(api=api_name, info='url')
    kafkast = KafkaStreamer(api_url=api_url, bootstrap_servers=bootstrap_servers)

    try:
        # Fetch data from API
        element = conf.get_api_info(api=api_name, info='element')
        raw_data = kafkast.get_data(element=element)

        # Retrieve mapping from api config and reformat the data
        mapping = conf.get_api_info(api=api_name, info='mapping')
        formatted_data = kafkast.format_data(data=raw_data, mapping=mapping)

        # Connect producer and stream data
        producer = kafkast.connect_producer()
        kafkast.stream_data(producer=producer, topic=topic, data=formatted_data)

    except Exception as e:
        logging.error(f'An error occurred during the "kafka streaming" job: {e}')
        raise


if __name__ == '__main__':

    api_name = 'randomuser'
    bootstrap_servers = 'broker:29092'
    topic = 'users_created'
    
    kafka_stream(api_name=api_name, bootstrap_servers=bootstrap_servers, topic=topic)

    