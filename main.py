import requests
from google.cloud import bigquery


from models.models import Orders
from controller.handler import run

from broadcast import broadcast

BQ_CLIENT = bigquery.Client()
SESSION = requests.Session()


def main(request):
    data = request.get_json()
    print(data)

    if data:
        if "broadcast" in data:
            job = broadcast()
        else:
            response = run(
                BQ_CLIENT,
                SESSION,
                Orders,
                data["auth"],
                data.get("start"),
                data.get("end"),
            )
    response = {
        "pipelines": "Shopify",
        "results": job,
    }
    print(response)
    return response
