# Stream Properties File Config
KAFKA_TOPICS = "kafka_topics"
KAFKA_SERVERS = "kafka_servers"
INPUT_TABLE_PATH = "input_table_path"
CHECKPOINT_LOCATION = "checkpoint_location"

# SPARK Config
JARS = "./Jars/delta-core_2.11-0.5.0.jar," \
       "./Jars/spark-sql-kafka-0-10_2.11-2.4.5.jar," \
       "./Jars/kafka-clients-2.4.1.jar"
APP_NAME = "DeltaKafkaWriter"
