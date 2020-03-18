from CardoSimulator.IStep import IStep


class CsvReader(IStep):
    def __init__(self, csv_path, header):
        super().__init__()
        self.header = header
        self.csv_path = csv_path

    def process(self, context, df=None):
        df = context.read.csv(self.csv_path, header=self.header)
        return df
