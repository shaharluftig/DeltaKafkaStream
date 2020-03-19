from Config import KAFKA_TOPICS, KAFKA_SERVERS, INPUT_TABLE_PATH, CHECKPOINT_LOCATION
from Steps.DeltaKafkaStream import DeltaKafkaStream


def execute_workflow(context, stream_config):
    df = DeltaKafkaStream(stream_config[INPUT_TABLE_PATH], stream_config[KAFKA_SERVERS], stream_config[KAFKA_TOPICS],
                          stream_config[CHECKPOINT_LOCATION]).process(context)
    return df
