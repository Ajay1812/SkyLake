from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

DELAY_REASON_COLUMNS = [
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY",
]

NUMERIC_COLUMNS = DELAY_REASON_COLUMNS + [
    "DEP_DELAY",
    "ARR_DELAY",
    "DISTANCE",
    "TAXI_OUT",
    "TAXI_IN",
    "CRS_ELAPSED_TIME",
    "ACTUAL_ELAPSED_TIME",
    "AIR_TIME",
]


class DataCleaner:
    def __init__(self, df) -> None:
        self.df = df

    def clean(self):
        # Drop column "Unnamed: 27"
        self.df = self.df.drop("Unnamed: 27")
        # Cast date(str) -> date
        self.df = self.df.withColumn('FL_DATE', F.to_date(self.df['FL_DATE'], 'yyyy-MM-dd'))
        # str -> Double
        for col in NUMERIC_COLUMNS:
            self.df = self.df.withColumn(col, F.col(col).cast(DoubleType()))

        self.df = self.df.withColumn("CANCELLED", F.col("CANCELLED").cast(DoubleType()).cast(IntegerType()))
        self.df = self.df.withColumn("DIVERTED", F.col("DIVERTED").cast(DoubleType()).cast(IntegerType()))

        # fillna
        self.df = self.df.fillna(0.0, subset=DELAY_REASON_COLUMNS)
        return self.df