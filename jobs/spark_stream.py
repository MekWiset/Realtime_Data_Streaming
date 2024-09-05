import logging
from plugins.spark_streamer import SparkStreamer
from plugins.utils.configparser import ConfigParser


def spark_stream(keyspace: str, topic: str, table_name: str, create_table_path: str, cluster: str = 'localhost', spark_host: str = 'localhost') -> None:

    '''
    Sets up a Spark streaming job to read data from Kafka and write it to a Cassandra table.

    This function performs the following steps:
    1. Loads configuration from a YAML file using `ConfigParser`.
    2. Initializes `SparkStreamer` with provided cluster and Spark host details.
    3. Creates a Cassandra keyspace and table based on provided names and SQL script.
    4. Connects to Kafka to read streaming data from the specified topic.
    5. Processes the Kafka data according to the specified schema.
    6. Writes the processed data to the Cassandra table in real-time.
    '''

    config_path = 'config.yaml'
    conf = ConfigParser(config_path=config_path)

    sparkst = SparkStreamer(cluster=cluster, spark_host=spark_host, config_parser=conf)

    try:
        # Create keyspace and table
        sparkst.create_keyspace(keyspace_name=keyspace)
        sparkst.create_table(create_table_path=create_table_path)

        # Connect to Kafka and insert data into topic
        kafka_df = sparkst.connect_to_kafka(host='localhost:9092', topic=topic)
        if kafka_df:
            selection_df = sparkst.create_selection_df_from_kafka(df=kafka_df, table_name=table_name)
            if selection_df:
                streaming_query = (selection_df.writeStream
                                   .format('org.apache.spark.sql.cassandra')
                                   .option('checkpointLocation', '/tmp/checkpoint')
                                   .option('keyspace', keyspace)
                                   .option('table', table_name)
                                   .outputMode('append')
                                   .start())
                
                streaming_query.awaitTermination()
                
    except Exception as e:
        logging.error(f'An error occurred during the Spark streaming job: {e}')
        raise


if __name__ == '__main__':

    keyspace = 'spark_streams'
    topic = 'users_created'
    table_name = 'created_users'
    create_table_path = 'helpers/create_table_created_users.sql'

    spark_stream(keyspace=keyspace, topic=topic, table_name=table_name, create_table_path=create_table_path)