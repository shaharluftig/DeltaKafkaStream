from Steps.CsvReader import CsvReader
from Steps.ElasticQSM import ElasticQSM


def execute_workflow(context):
    df = CsvReader("Datasets/covid_19_data.csv", True).process(context).persist()
    df = ElasticQSM("try_index").process(df, context)
    return df
