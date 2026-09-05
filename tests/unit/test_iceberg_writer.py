from unittest.mock import MagicMock

from spark.iceberg_writer import IcebergWriter


def _make_spark(table_exists):
    spark = MagicMock()
    spark.catalog.tableExists.return_value = table_exists
    return spark


def test_write_iceberg_appends_when_table_exists():
    spark = _make_spark(table_exists=True)
    df = MagicMock()

    IcebergWriter(spark, df, "glue", "flights").write_iceberg()

    spark.catalog.tableExists.assert_called_once_with("glue.flights.flight_events")
    df.writeTo.assert_called_once_with("glue.flights.flight_events")
    df.writeTo.return_value.using.assert_called_once_with("iceberg")
    df.writeTo.return_value.using.return_value.append.assert_called_once()
    df.writeTo.return_value.using.return_value.create.assert_not_called()


def test_write_iceberg_creates_when_table_missing():
    spark = _make_spark(table_exists=False)
    df = MagicMock()

    IcebergWriter(spark, df, "glue", "flights").write_iceberg()

    df.writeTo.return_value.using.return_value.create.assert_called_once()
    df.writeTo.return_value.using.return_value.append.assert_not_called()


def test_write_iceberg_builds_table_name_from_catalog_and_db():
    spark = _make_spark(table_exists=True)
    df = MagicMock()

    IcebergWriter(spark, df, "custom_catalog", "custom_db").write_iceberg()

    spark.catalog.tableExists.assert_called_once_with("custom_catalog.custom_db.flight_events")
    df.writeTo.assert_called_once_with("custom_catalog.custom_db.flight_events")
