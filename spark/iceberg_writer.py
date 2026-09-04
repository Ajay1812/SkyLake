class IcebergWriter:
    def __init__(self, spark, df, catalog, db):
        self.spark = spark
        self.df = df
        self.catalog = catalog
        self.db = db

    def write_iceberg(self):
        table = f'{self.catalog}.{self.db}.flight_events'
        if self.spark.catalog.tableExists(table):
            self.df.writeTo(table).using('iceberg').append()
        else:
            self.df.writeTo(table).using('iceberg').create()