from spark.reader import DataReader


def test_read_flights_returns_expected_columns(spark, sample_flights_csv):
    df = DataReader(spark, sample_flights_csv).read_flights()
    assert "FL_DATE" in df.columns
    assert "OP_CARRIER" in df.columns


def test_read_flights_row_count(spark, sample_flights_csv):
    df = DataReader(spark, sample_flights_csv).read_flights()
    assert df.count() == 3
