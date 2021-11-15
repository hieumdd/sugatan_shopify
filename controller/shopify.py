from typing import Callable, Optional, TypedDict

import requests

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

API_VER = "2021-10"


class Auth(TypedDict):
    client: str
    api_key: str
    api_secret: str
    shop_url: str


ParamsBuilder = Callable[[str, str], dict]

Fetcher = Callable[
    [
        requests.Session,
        Auth,
        dict,
        Optional[str],
    ],
    list[dict],
]

Getter = Callable[
    [
        requests.Session,
        Auth,
        str,
        str,
    ],
    tuple[Optional[Exception], Optional[list[dict]]],
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


def fetch(endpoint: str, data_key: str) -> Fetcher:
    def _fetch(
        session: requests.Session,
        auth: Auth,
        params: dict,
        url: str = None,
    ) -> list[dict]:
        _url = (
            url
            if url
            else f"https://{auth['api_key']}:{auth['api_secret']}@{auth['shop_url']}/admin/api/{API_VER}/{endpoint}"
        )
        with session.get(_url, params=params) as r:
            res = r.json()
            next_link = r.links.get("next")
        data = res[data_key]
        return (
            data
            + _fetch(
                session,
                auth,
                {**limit},
                next_link.get("url").replace(
                    auth["shop_url"],
                    f"{auth['api_key']}:{auth['api_secret']}@{auth['shop_url']}",
                ),
            )
            if next_link
            else data
        )

    return _fetch


def get(fetcher: Fetcher, params_builder: ParamsBuilder) -> Getter:
    def _get(
        session: requests.Session,
        auth: Auth,
        start: str,
        end: str,
    ) -> tuple[Optional[Exception], Optional[list[dict]]]:
        try:
            return None, fetcher(
                session,
                auth,
                params_builder(start, end),
                None,
            )
        except Exception as e:
            return e, None

    return _get
