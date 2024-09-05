from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from typing import Optional, Any
import logging

from plugins.utils.configparser import ConfigParser


class CassandraConnector:

    '''
    Manages connections to a Cassandra database and performs schema management.

    Args:
        cluster (str): The address of the Cassandra cluster.

    Methods:
        create_connection(self) -> Optional[Cluster]:
            Creates and returns a connection to the Cassandra cluster.
        
        create_keyspace(self, keyspace_name: str) -> None:
            Creates a keyspace in Cassandra with the specified name if it doesn't already exist.
        
        create_table(self, create_table_path: str) -> None:
            Creates a table in Cassandra based on the SQL script at the specified path.
    '''
    
    def __init__(self, cluster: str) -> None:
        '''Initialize the CassandraConnector with the cluster address and create a Cassandra connection.'''
        self.cluster = cluster
        self.session = self.create_connection()

    def create_connection(self) -> Optional[Cluster]:
        '''Create and return a connection to the Cassandra cluster.'''
        try:
            cluster = Cluster([self.cluster])
            session = cluster.connect()
            logging.info('Cassandra connection created successfully!')
            return session
        except Exception as e:
            logging.error(f'Could not create Cassandra connection: {e}')
            return None

    def create_keyspace(self, keyspace_name: str) -> None:
        '''Create a keyspace in Cassandra with the specified name if it doesn't already exist.'''
        try:
            self.session.execute(
                f'''
                CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
                '''
            )
            logging.info('Keyspace created successfully!')
        except Exception as e:
            logging.error(f'Could not create keyspace: {e}')

    def create_table(self, create_table_path: str) -> None:
        '''Create a table in Cassandra based on the SQL script at the specified path.'''
        try:
            with open(create_table_path, 'r') as file:
                create_table_query = file.read()
            self.session.execute(create_table_query)
            logging.info("Table created successfully!")
        except Exception as e:
            logging.error(f'Could not create table: {e}')


class SparkStreamer(CassandraConnector):

    '''
    Streams data from Kafka into Cassandra using Spark.

    Args:
        cluster (str): The address of the Cassandra cluster.
        spark_host (str): The address of the Spark host.
        config_parser (ConfigParser): Configuration parser for database and table schemas.

    Methods:
        create_spark_connection(self) -> SparkSession:
            Creates and returns a Spark session configured for Cassandra and Kafka.
        
        connect_to_kafka(self, host: str, topic: str) -> Optional[Any]:
            Connects to a Kafka topic and returns a DataFrame of the stream data.
        
        create_selection_df_from_kafka(self, df: dict, db: str, table_name: str) -> Optional[Any]:
            Creates a DataFrame from Kafka stream data based on the schema retrieved from the configuration.
        
        insert_data(self, db: str, table_name: str, **kwargs: Any) -> None:
            Inserts data into a Cassandra table using values provided in kwargs.
    '''

    def __init__(self, cluster: str, spark_host: str, config_parser: ConfigParser) -> None:
        '''Initialize SparkStreamer with Cassandra cluster address, Spark host, and configuration parser, and create a Spark session.'''
        super().__init__(cluster)
        self.spark_host = spark_host
        self.config_parser = config_parser
        self.spark_session = self.create_spark_connection()

    def create_spark_connection(self) -> SparkSession:
        '''Create and return a Spark session configured for Cassandra and Kafka.'''
        try:
            spark = SparkSession \
                .builder \
                .appName('SparkDataStreaming') \
                .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1') \
                .config('spark.cassandra.connection.host', self.spark_host) \
                .getOrCreate()
            spark.sparkContext.setLogLevel('ERROR')
            logging.info('Spark connection created successfully!')
            return spark
        except Exception as e:
            logging.error(f'Could not create Spark session: {e}')
            return None

    def connect_to_kafka(self, host: str, topic: str) -> Optional[Any]:
        '''Connect to a Kafka topic and return a DataFrame with the stream.'''
        try:
            kafka_df = self.spark_session.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', host) \
                .option('subscribe', topic) \
                .option('startingOffsets', 'earliest') \
                .load()
            logging.info("Kafka dataframe created successfully")
            return kafka_df
        except Exception as e:
            logging.error(f"Could not create Kafka dataframe: {e}")
            return None

    def create_selection_df_from_kafka(self, df: dict, db: str, table_name: str) -> Optional[Any]:
        '''Create a DataFrame from Kafka data using the specified schema.'''
        try:
            schema = self.config_parser.get_db_info(db, table_name, 'schema')
            sel = df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col('value'), schema).alias('data')) \
                .select("data.*")
            logging.info("Selection dataframe created successfully")
            return sel
        except Exception as e:
            logging.error(f"Could not create selection dataframe: {e}")
            return None

    def insert_data(self, db: str, table_name: str, **kwargs: Any) -> None:
        '''Insert data into a Cassandra table with the provided values.'''
        try:
            columns_placeholders = self.config_parser.get_column_placeholders(db, table_name)
            values_placeholders = self.config_parser.get_values_placeholders(db, table_name)
            
            insert_query = f'''
                INSERT INTO {db}.{table_name}({columns_placeholders})
                VALUES ({values_placeholders})
            '''
            
            values = self.config_parser.get_column_values(db, table_name, **kwargs)
            
            if self.session:
                self.session.execute(insert_query, values)
                logging.info(f"Data inserted for {kwargs.get('first_name')} {kwargs.get('last_name')}")

        except Exception as e:
            logging.error(f'Could not insert data: {e}')