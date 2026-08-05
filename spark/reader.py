class DataReader:
    def __init__(self, spark, path) -> None:
        self.spark = spark
        self.path = path

    def read_flights(self):
        df = self.spark.read.csv(self.path, header=True)
        return df
