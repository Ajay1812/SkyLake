import pytest

from spark.config import Session


@pytest.fixture(autouse=True)
def no_dotenv(monkeypatch):
    # Prevent a real .env file on disk from filling in vars a test deletes.
    monkeypatch.setattr("spark.config.load_dotenv", lambda: None)


def _set_required_env(monkeypatch, **overrides):
    values = {
        "AWS_REGION": "us-east-1",
        "S3_BUCKET": "my-bucket",
        "CATALOG": "glue",
        "DATABASE": "flights",
    }
    values.update(overrides)
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def test_session_reads_env_vars(monkeypatch):
    _set_required_env(monkeypatch)

    session = Session()

    assert session.REGION == "us-east-1"
    assert session.BUCKET == "my-bucket"
    assert session.CATALOG == "glue"
    assert session.DATABASE == "flights"


def test_session_derives_paths_from_bucket(monkeypatch):
    _set_required_env(monkeypatch, S3_BUCKET="other-bucket")

    session = Session()

    assert session.PROCESSED_PATH == "s3a://other-bucket/processed/flights_cleaned"
    assert session.ICEBERG_WAREHOUSE == "s3a://other-bucket/iceberg"


def test_session_spark_is_none_until_started(monkeypatch):
    _set_required_env(monkeypatch)

    session = Session()

    assert session.spark is None


@pytest.mark.parametrize("missing_var", ["AWS_REGION", "S3_BUCKET", "CATALOG", "DATABASE"])
def test_session_requires_each_env_var(monkeypatch, missing_var):
    _set_required_env(monkeypatch)
    monkeypatch.delenv(missing_var, raising=False)

    with pytest.raises(KeyError):
        Session()
