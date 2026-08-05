class IcebergWriter:
    def __init__(self, spark, df, catalog, db):
        self.spark = spark
        self.df = df
        self.catalog = catalog
        self.db = db
        
    def write_iceberg(self):
        self.df.writeTo(f'{self.catalog}.{self.db}.flight_events')\
                .using('iceberg')\
                .createOrReplace()