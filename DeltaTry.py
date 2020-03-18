from pyspark.sql import SparkSession

JARS = "./Jars/delta-core_2.11-0.5.0.jar," \
       "./Jars/spark-sql-kafka-0-10_2.11-2.4.5.jar," \
       "./Jars/kafka-clients-2.4.1.jar"

context = SparkSession.builder.master("local").config("spark.jars", JARS).appName("ElasticQSM").getOrCreate()

df = context.readStream.format("delta").load("./HdfsElasticQSM/try_index_stage_0")

query = df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("checkpointLocation", "./checkpoint") \
    .option("topic", "test").start()

# df = context.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe",
#                                                                                                    "test").load()
#
# stream = df.selectExpr("CAST(value AS STRING)").select("value").writeStream.format("console").outputMode(
#     "append").start()
# stream.awaitTermination()
