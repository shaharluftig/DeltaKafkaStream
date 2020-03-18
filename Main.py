from pyspark.sql import SparkSession

from Config import JARS, APP_NAME
from Workflow import execute_workflow

if __name__ == '__main__':
    context = SparkSession.builder.master("local").config("spark.jars", JARS).appName(APP_NAME).getOrCreate()
    execute_workflow(context)
