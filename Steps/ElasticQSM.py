import secrets

from pyspark.sql import functions as F, Window

from CardoSimulator.IStep import IStep


class ElasticQSM(IStep):
    def __init__(self, desired_index, rows_per_stage_table=1000, to_split=True,
                 path="./HdfsElasticQSM"):
        super().__init__()
        self.desired_index = desired_index
        self.rows_per_stage_table = rows_per_stage_table
        self.to_split = to_split
        self.path = path

    def process(self, df, context):
        num_rows = df.count()
        columns_map = self.get_columns_map(df)
        if self.to_split and num_rows >= self.rows_per_stage_table:
            dataframes = self.split_to_dataframes(df, num_rows)
            self.write_stage_tables(dataframes, columns_map)
            return df
        self.write_single_stage_table(df, columns_map)
        return df

    def get_columns_map(self, df):
        columns_map = []
        for column in df.columns:
            columns_map.append(F.lit(column))
            columns_map.append(F.col(column))
        return columns_map

    def split_to_dataframes(self, df, num_rows):
        df = df.withColumn("_rownum", F.monotonically_increasing_id())
        num_partitions = num_rows // self.rows_per_stage_table
        df = df.withColumn('_partition', F.ntile(num_partitions).over(Window.orderBy(F.col("_rownum"))))
        return [df.filter(df._partition == i + 1).drop('_rownum', '_partition') for i in range(num_partitions)]

    def write_stage_tables(self, dataframes, columns_map):
        for df in dataframes:
            df = df.select(F.to_json(F.create_map(*columns_map)).alias("value"))
            df.write.format("delta").mode("overwrite").save(
                self.path + "/" + self.desired_index + "/" + self.desired_index + "_stage_"
                + str(secrets.token_hex(nbytes=8)))

    def write_single_stage_table(self, df, columns_map):
        df = df.select(F.to_json(F.create_map(*columns_map)).alias("value"))
        df.write.format("delta").mode("overwrite").save(
            self.path + "/" + self.desired_index + "/" + self.desired_index + "_stage")
