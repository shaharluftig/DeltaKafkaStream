from pyspark.sql import SparkSession

from Config import JARS
from Workflow import execute_workflow

if __name__ == '__main__':
    context = SparkSession.builder.master("local").config("spark.jars", JARS).appName("DeltaKafkaWriter").getOrCreate()
    execute_workflow(context)
    print("Done")
