from typing import TypedDict
import os
import uuid
import json

from google.cloud import secretmanager, tasks_v2

from controller.shopify import Auth


TASKS_CLIENT = tasks_v2.CloudTasksClient()
SECRET_CLIENT = secretmanager.SecretManagerServiceClient()


class RawAuth(TypedDict):
    client: str
    shop_url: str
    api_key: tuple[str, int]
    api_secret: tuple[str, int]


CLIENTS: list[RawAuth] = [
    {
        "client": "SBLA",
        "shop_url": "spencer-barnes.myshopify.com",
        "api_key": ("shopify_SBLA_api_key", 1),
        "api_secret": ("shopify_SBLA_api_secret", 1),
    },
    {
        "client": "SniffAndBark",
        "shop_url": "sniffandbark.myshopify.com",
        "api_key": ("shopify_sniffandbark_api_key", 1),
        "api_secret": ("shopify_sniffandbark_api_secret", 1),
    },
]


def get_secret(
    secret_client: secretmanager.SecretManagerServiceClient,
    secret_id: str,
    version_id: int,
) -> str:
    return secret_client.access_secret_version(
        request={
            "name": f"projects/{os.getenv('PROJECT_ID')}/secrets/{secret_id}/versions/{version_id}"
        }
    ).payload.data.decode("UTF-8")


def build_auth(
    secret_client: secretmanager.SecretManagerServiceClient,
    client: RawAuth,
) -> Auth:
    return {
        "client": client["client"],
        "shop_url": client["shop_url"],
        "api_key": get_secret(
            secret_client,
            client["api_key"][0],
            client["api_key"][1],
        ),
        "api_secret": get_secret(
            secret_client,
            client["api_secret"][0],
            client["api_secret"][1],
        ),
    }


AUTHS = [build_auth(SECRET_CLIENT, i) for i in CLIENTS]


def create_tasks(tasks_data: dict) -> dict:
    tasks_path = (os.getenv("PROJECT_ID", ""), "us-central1", "shopify")
    payloads = [
        {
            "name": f"{auth['client']}-{uuid.uuid4()}",
            "payload": {
                "auth": auth,
                "start": tasks_data.get("start"),
                "end": tasks_data.get("end"),
            },
        }
        for auth in AUTHS
    ]
    tasks = [
        {
            "name": TASKS_CLIENT.task_path(*tasks_path, task=f"{payload['name']}"),
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": os.getenv("PUBLIC_URL"),
                "oidc_token": {
                    "service_account_email": os.getenv("GCP_SA"),
                },
                "headers": {
                    "Content-type": "application/json",
                },
                "body": json.dumps(payload["payload"]).encode(),
            },
        }
        for payload in payloads
    ]
    responses = [
        TASKS_CLIENT.create_task(
            request={
                "parent": TASKS_CLIENT.queue_path(*tasks_path),
                "task": task,
            }
        )
        for task in tasks
    ]
    return {
        "messages_sent": len(responses),
        "tasks_data": tasks_data,
    }
