import json
import os
import urllib.request


def task_failure_alert(context):
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id

    message = (
        f"DAG Failed!\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_instance.task_id}\n"
        f"Execution Time: {context['execution_date']}"
    )
    print(message)
    _notify_slack(message)


def _notify_slack(message):
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    request = urllib.request.Request(
        webhook_url,
        data=json.dumps({"text": message}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(request, timeout=5)
    except Exception as exc:
        print(f"Failed to send Slack alert: {exc}")
