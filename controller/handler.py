from typing import Optional

import requests
from google.cloud import bigquery

from models.shopify import ShopifyResource

BQ_CLIENT = bigquery.Client()
SESSION = requests.Session()


def run(
    model: ShopifyResource,
    auth: dict,
    start: str,
    end: str,
) -> tuple[Optional[Exception], Optional[dict]]:
    dataset = f"{auth['client']}_Shopify"
    _start, _end = model["time_range_getter"](BQ_CLIENT, dataset, start, end)
    err_data, data = model["getter"](SESSION, auth, _start, _end)
    if err_data and not data:
        return err_data, data
    else:
        response = {
            "start": _start,
            "end": _end,
            "num_processed": len(data),
        }
        if len(data) > 0:
            response["output_rows"] = model["loader"](
                BQ_CLIENT,
                dataset,
                model["transform"](data),
            )
        return None, response
