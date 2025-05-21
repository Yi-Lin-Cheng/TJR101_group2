import os

import requests
from airflow.models import Variable


def line_notify_failure(context):
    token = Variable.get("LINE_CHANNEL_ACCESS_TOKEN")
    group_id = Variable.get("LINE_ALERT_GROUP_ID")

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exec_time = context["execution_date"]
    log_url = context["task_instance"].log_url

    message = f"""
Airflow 任務失敗通知
DAG: {dag_id}
Task: {task_id}
時間: {exec_time}
Log: {log_url}
"""

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    payload = {"to": group_id, "messages": [{"type": "text", "text": message.strip()}]}

    try:
        res = requests.post(
            "https://api.line.me/v2/bot/message/push", headers=headers, json=payload
        )
        res.raise_for_status()
        print("LINE 推播成功")
    except Exception as e:
        print("發送 LINE 失敗：", e)
