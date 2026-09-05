from airflow.exceptions import AirflowSkipException

from spark import cleaner, config, iceberg_writer, reader, writer
from spark.manifest import ManifestManager


def extract_clean_and_write_parquet():
    session = config.Session()
    manifest = ManifestManager(session.BUCKET, session.REGION)
    new_files = manifest.get_new_files()
    if not new_files:
        raise AirflowSkipException("No new raw files to process")

    spark = session.get_spark_session()
    print('Spark Session Created')
    try:
        paths = [f"s3a://{session.BUCKET}/raw/{f}" for f in new_files]
        flight_df = reader.DataReader(spark, paths).read_flights()

        data_cleaner = cleaner.DataCleaner(flight_df)
        cleaned_data = data_cleaner.clean()

        data = writer.DataWriter(cleaned_data)
        data.write_parquet(session.PROCESSED_PATH)

        manifest.mark_processed(new_files)
    finally:
        session.stop()
        print('Session Stopped')

def write_iceberg_table():
    session = config.Session()
    spark = session.get_spark_session()
    print('Spark Session Created')
    try:
        cleaned_data = spark.read.parquet(session.PROCESSED_PATH)
        cleaned_data.show(5)
        iceberg_data = iceberg_writer.IcebergWriter(
                            spark=spark,
                            df=cleaned_data,
                            db=session.DATABASE,
                            catalog=session.CATALOG
        )
        iceberg_data.write_iceberg()
    finally:
        session.stop()
        print('Session Stopped')

def main():
    extract_clean_and_write_parquet()
    write_iceberg_table()

if __name__ == "__main__":
    main()
