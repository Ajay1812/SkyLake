from spark import config, reader, cleaner, writer, iceberg_writer

def extract_clean_and_write_parquet():
    session = config.Session()
    spark = session.get_spark_session()
    print('Spark Session Created')
    try:
        flight_df = reader.DataReader(spark, session.RAW_PATH).read_flights()
        # flight_df.show(5)

        data_cleaner = cleaner.DataCleaner(flight_df)
        cleaned_data = data_cleaner.clean()

        data = writer.DataWriter(cleaned_data)
        data.write_parquet(session.PROCESSED_PATH)
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
