class DataWriter:
    def __init__(self, df):
        self.df = df

    def write_parquet(self, path):
        self.df.write.mode('overwrite').parquet(path)