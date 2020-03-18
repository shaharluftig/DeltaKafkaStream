from pyspark.sql import SparkSession

from Workflow import execute_workflow

if __name__ == '__main__':
    context = SparkSession.builder.master("local").config("spark.jars", "./Jars/delta-core_2.11-0.5.0.jar").appName(
        "ElasticQSM").getOrCreate()
    execute_workflow(context)
    print("Done")
