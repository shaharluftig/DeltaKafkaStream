from CardoSimulator.IStep import IStep


class DeltaKafkaStream(IStep):
    def __init__(self, path, servers, topics, checkpoint_location="./checkpoint"):
        super().__init__()
        self.checkpoint_location = checkpoint_location
        self.topics = topics
        self.servers = servers
        self.path = path

    def process(self, context, df=None):
        df_stream = context.readStream.format("delta").load(self.path)
        query = df_stream.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.servers) \
            .option("checkpointLocation", self.checkpoint_location) \
            .option("topic", self.topics).start()
        return query.awaitTermination()
