from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

class Session:
    def __init__(self) -> None:
        load_dotenv()
        self.REGION = os.environ['AWS_REGION']
        self.BUCKET = os.environ['S3_BUCKET']
        self.RAW_PATH = f"s3a://{self.BUCKET}/raw/*.csv"
        self.PROCESSED_PATH = f"s3a://{self.BUCKET}/processed/flights_cleaned"
        self.ICEBERG_WAREHOUSE = f"s3a://{self.BUCKET}/iceberg"
        self.CATALOG = "local"
        self.DATABASE = "flights"

    def get_spark_session(self):
        self.spark = SparkSession.builder.master('local[4]')\
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.2,org.apache.iceberg:iceberg-aws-bundle:1.10.2")\
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")\
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")\
                .config("spark.sql.catalog.local.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")\
                .config("spark.sql.catalog.local.warehouse", self.ICEBERG_WAREHOUSE)\
                .appName('skylake').getOrCreate()
        return self.spark

    def stop(self):
        self.spark.stop()
