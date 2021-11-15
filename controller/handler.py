from typing import Optional

import requests
from google.cloud import bigquery

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
    _start, _end = model["time_range_getter"](client, dataset, start, end)
    err_data, data = model["getter"](session, auth, _start, _end)
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
            response["output_rows"] = model["loader"](
                client,
                dataset,
                model["transform"](data),
            )
        return None, response
