from datetime import datetime, timedelta
from typing import Callable, Optional, TypedDict

import requests

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


class Auth(TypedDict):
    client: str
    api_key: str
    api_secret: str
    shop_url: str


ParamsBuilder = Callable[[str, str], dict]

Getter = Callable[
    [
        requests.Session,
        Auth,
        dict,
        Optional[str],
    ],
    list[dict],
]


limit = {
    "limit": 250,
}


def build_params(fields: list[str]) -> ParamsBuilder:
    def build(start: str, end: str) -> dict:
        return {
            **limit,
            "fields": ",".join(fields),
            "status": "any",
            "updated_at_min": start,
            "updated_at_max": end,
        }

    return build


def get(api_ver: str, endpoint: str) -> Getter:
    def _get(
        session: requests.Session,
        auth: Auth,
        params: dict,
        url: str = None,
    ) -> list[dict]:
        _url = (
            url
            if url
            else f"https://{auth['api_key']}:{auth['api_secret']}@{auth['shop_url']}/admin/api/{api_ver}/{endpoint}"
        )
        with session.get(_url, params=params) as r:
            res = r.json()
            next_link = r.links.get("next")
        orders = res["orders"]
        return (
            orders
            + _get(
                session,
                auth=auth,
                params={**limit},
                url=next_link.get("url").replace(
                    auth["shop_url"],
                    f"{auth['api_key']}:{auth['api_secret']}@{auth['shop_url']}",
                ),
            )
            if next_link
            else orders
        )

    return _get


def get_data(
    session: requests.Session,
    api_ver: str,
    endpoint: str,
    fields: list[str],
    auth: Auth,
    start: str,
    end: str,
):
    try:
        return None, get(api_ver, endpoint)(
            session,
            auth,
            {
                **limit,
                "fields": ",".join(fields),
                "status": "any",
                "updated_at_min": start,
                "updated_at_max": end,
            },
            None,
        )
    except Exception as e:
        return e, None
