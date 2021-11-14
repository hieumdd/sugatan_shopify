from typing import Optional

import requests
from google.cloud import bigquery

from controller.shopify import get_data
from controller.bigquery import get_time_range
from models.models import ShopifyResource


def run(
    client: bigquery.Client,
    session: requests.Session,
    model: ShopifyResource,
    auth: dict,
    start: str,
    end: str,
) -> tuple[Optional[Exception], Optional[dict]]:
    dataset = f"{auth['client']}_Shopify"
    _start, _end = get_time_range(client, dataset, model["table"], start, end)
    err_data, data = get_data(
        session,
        model['api_ver'],
        model['endpoint'],
        model["fields"],
        auth,
        _start,
        _end,
    )
    if err_data and not data:
        return err_data, data
    else:
        response = {
            "table": model["table"],
            "start": _start,
            "end": _end,
            "num_processed": len(data),
        }
        if len(data) > 0:
            response["output_rows"] = model['load'](
                client,
                dataset,
                model["table"],
                model["transform"](data),
            )
        return None, response
