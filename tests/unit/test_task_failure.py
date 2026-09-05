import json
from unittest.mock import MagicMock

from spark.task_failure import task_failure_alert


def _make_context():
    task_instance = MagicMock()
    task_instance.task_id = "write_iceberg_table"
    dag = MagicMock()
    dag.dag_id = "flight_events"
    return {
        "task_instance": task_instance,
        "dag": dag,
        "execution_date": "2026-09-05T00:00:00",
    }


def test_task_failure_alert_skips_slack_when_webhook_unset(monkeypatch, capsys):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    urlopen = MagicMock()
    monkeypatch.setattr("spark.task_failure.urllib.request.urlopen", urlopen)

    task_failure_alert(_make_context())

    urlopen.assert_not_called()
    assert "flight_events" in capsys.readouterr().out


def test_task_failure_alert_posts_to_slack_when_webhook_set(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/webhook")
    urlopen = MagicMock()
    monkeypatch.setattr("spark.task_failure.urllib.request.urlopen", urlopen)

    task_failure_alert(_make_context())

    urlopen.assert_called_once()
    request = urlopen.call_args[0][0]
    assert request.full_url == "https://hooks.slack.test/webhook"
    payload = json.loads(request.data.decode("utf-8"))
    assert "flight_events" in payload["text"]
    assert "write_iceberg_table" in payload["text"]


def test_task_failure_alert_swallows_slack_errors(monkeypatch, capsys):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/webhook")
    urlopen = MagicMock(side_effect=OSError("network down"))
    monkeypatch.setattr("spark.task_failure.urllib.request.urlopen", urlopen)

    task_failure_alert(_make_context())  # must not raise

    assert "Failed to send Slack alert" in capsys.readouterr().out
