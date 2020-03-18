class IStep():
    def __init__(self):
        pass

    def process(self, df, context):
        raise NotImplementedError()
