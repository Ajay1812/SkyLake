from spark.cleaner import DataCleaner
from spark.reader import DataReader


def test_clean_drops_unnamed_column(spark, sample_flights_csv):
    df = DataReader(spark, sample_flights_csv).read_flights()
    cleaned = DataCleaner(df).clean()
    assert "Unnamed: 27" not in cleaned.columns


def test_clean_casts_types(spark, sample_flights_csv):
    df = DataReader(spark, sample_flights_csv).read_flights()
    cleaned = DataCleaner(df).clean()
    dtypes = dict(cleaned.dtypes)
    assert dtypes["FL_DATE"] == "date"
    assert dtypes["ARR_DELAY"] == "double"
    assert dtypes["CANCELLED"] == "int"
    assert dtypes["DIVERTED"] == "int"


def test_clean_fills_null_delay_reasons(spark, sample_flights_csv):
    df = DataReader(spark, sample_flights_csv).read_flights()
    cleaned = DataCleaner(df).clean()
    cancelled_row = cleaned.filter(cleaned.OP_CARRIER == "DL").first()
    assert cancelled_row["CARRIER_DELAY"] == 0.0
    assert cancelled_row["WEATHER_DELAY"] == 0.0
    assert cancelled_row["NAS_DELAY"] == 0.0


def test_clean_preserves_row_count(spark, sample_flights_csv):
    df = DataReader(spark, sample_flights_csv).read_flights()
    cleaned = DataCleaner(df).clean()
    assert cleaned.count() == df.count() == 3
