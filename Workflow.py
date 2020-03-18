from Config import TOPICS, SERVERS, PATH
from Steps.DeltaKafkaStream import DeltaKafkaStream


def execute_workflow(context):
    df = DeltaKafkaStream(PATH, SERVERS, TOPICS).process(context)
    return df
