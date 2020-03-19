import json

from pyspark.sql import SparkSession

from Config import JARS, APP_NAME
from Workflow import execute_workflow

STREAM_PROPERTIES_JSON = "StreamProperties.json"

if __name__ == '__main__':
    stream_config = json.load(open(STREAM_PROPERTIES_JSON))
    context = SparkSession.builder.master("local").config("spark.jars", JARS).appName(APP_NAME).getOrCreate()
    execute_workflow(context, stream_config)
