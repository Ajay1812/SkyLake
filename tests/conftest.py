from pathlib import Path

import pytest
from pyspark.sql import SparkSession

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("skylake-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_flights_csv():
    return str(FIXTURES_DIR / "sample_flights.csv")
