import io
import json

from botocore.response import StreamingBody
from botocore.stub import Stubber

from spark.manifest import ManifestManager


def _json_body(payload):
    data = json.dumps(payload).encode("utf-8")
    return StreamingBody(io.BytesIO(data), len(data))


def test_get_new_files_returns_files_not_in_manifest():
    manager = ManifestManager(bucket="my-bucket", region="us-east-1")
    stubber = Stubber(manager.s3)
    stubber.add_response(
        "get_object",
        {"Body": _json_body(["a.csv"])},
        {"Bucket": "my-bucket", "Key": "raw/_manifest.json"},
    )
    stubber.add_response(
        "list_objects_v2",
        {
            "Contents": [
                {"Key": "raw/a.csv"},
                {"Key": "raw/b.csv"},
                {"Key": "raw/notes.txt"},
            ]
        },
        {"Bucket": "my-bucket", "Prefix": "raw/"},
    )

    with stubber:
        new_files = manager.get_new_files()

    assert new_files == ["b.csv"]
    stubber.assert_no_pending_responses()


def test_get_new_files_returns_all_files_when_manifest_missing():
    manager = ManifestManager(bucket="my-bucket", region="us-east-1")
    stubber = Stubber(manager.s3)
    stubber.add_client_error(
        "get_object",
        service_error_code="NoSuchKey",
        expected_params={"Bucket": "my-bucket", "Key": "raw/_manifest.json"},
    )
    stubber.add_response(
        "list_objects_v2",
        {"Contents": [{"Key": "raw/a.csv"}]},
        {"Bucket": "my-bucket", "Prefix": "raw/"},
    )

    with stubber:
        new_files = manager.get_new_files()

    assert new_files == ["a.csv"]
    stubber.assert_no_pending_responses()


def test_get_new_files_returns_nothing_when_all_processed():
    manager = ManifestManager(bucket="my-bucket", region="us-east-1")
    stubber = Stubber(manager.s3)
    stubber.add_response(
        "get_object",
        {"Body": _json_body(["a.csv"])},
        {"Bucket": "my-bucket", "Key": "raw/_manifest.json"},
    )
    stubber.add_response(
        "list_objects_v2",
        {"Contents": [{"Key": "raw/a.csv"}]},
        {"Bucket": "my-bucket", "Prefix": "raw/"},
    )

    with stubber:
        new_files = manager.get_new_files()

    assert new_files == []


def test_mark_processed_merges_and_sorts_filenames():
    manager = ManifestManager(bucket="my-bucket", region="us-east-1")
    stubber = Stubber(manager.s3)
    stubber.add_response(
        "get_object",
        {"Body": _json_body(["a.csv"])},
        {"Bucket": "my-bucket", "Key": "raw/_manifest.json"},
    )
    stubber.add_response(
        "put_object",
        {},
        {
            "Bucket": "my-bucket",
            "Key": "raw/_manifest.json",
            "Body": json.dumps(["a.csv", "b.csv", "c.csv"]).encode("utf-8"),
        },
    )

    with stubber:
        manager.mark_processed(["c.csv", "b.csv"])

    stubber.assert_no_pending_responses()


def test_mark_processed_starts_fresh_when_manifest_missing():
    manager = ManifestManager(bucket="my-bucket", region="us-east-1")
    stubber = Stubber(manager.s3)
    stubber.add_client_error(
        "get_object",
        service_error_code="NoSuchKey",
        expected_params={"Bucket": "my-bucket", "Key": "raw/_manifest.json"},
    )
    stubber.add_response(
        "put_object",
        {},
        {
            "Bucket": "my-bucket",
            "Key": "raw/_manifest.json",
            "Body": json.dumps(["a.csv"]).encode("utf-8"),
        },
    )

    with stubber:
        manager.mark_processed(["a.csv"])

    stubber.assert_no_pending_responses()
