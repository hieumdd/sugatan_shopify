import os
import json
import base64
from datetime import datetime, timedelta

import requests
import jinja2
from google.cloud import bigquery

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


class ShopifyOrdersJob:
    def __init__(self, **kwargs):
        self.API_KEY = os.getenv("API_KEY")
        self.API_SECRET = os.getenv("API_SECRET")
        self.API_VER = os.getenv("API_VER")
        self.SHOP_URL = os.getenv("SHOP_URL")

        BUSINESS = os.getenv("BUSINESS")
        self.dataset = f"{BUSINESS}_Shopify"

        self.TABLE = "Orders"

        self.client = bigquery.Client()

        now = datetime.now()
        self.end_date = kwargs.get("end_date", now.strftime(TIMESTAMP_FORMAT))
        self.start_date = kwargs.get(
            "start_date", (now - timedelta(days=30)).strftime(TIMESTAMP_FORMAT)
        )

        with open("config.json", "r") as f:
            config = json.load(f)
        self.fields = config.get("fields")
        self.encoded_fields = config.get("encoded_fields")

    def fetch(self):
        INITIAL_URL = f"https://{self.API_KEY}:{self.API_SECRET}@{self.SHOP_URL}/admin/api/{self.API_VER}/orders.json"

        orders = []
        with requests.Session() as session:
            params = {
                "limit": 250,
                "status": "any",
                "updated_at_min": self.start_date,
                "updated_at_max": self.end_date,
                "fields": ",".join(self.fields),
            }
            url = INITIAL_URL
            while url:
                with session.get(url, params=params) as r:
                    res = r.json()
                    orders.extend(res.get("orders"))
                    next_link = r.links.get("next")
                    if next_link:
                        url = next_link.get("url")
                        url = url.replace(
                            self.SHOP_URL,
                            f"{self.API_KEY}:{self.API_SECRET}@{self.SHOP_URL}",
                        )
                        print(len(orders))
                    else:
                        url = None
                params = {"limit": 250}

        return orders

    def transform(self, rows):
        for i in rows:
            for f in self.encoded_fields:
                i[f] = json.dumps(i[f])
        self.num_processed = len(rows)
        return rows

    def load(self, rows):
        with open("schema.json", "r") as f:
            schema = json.load(f)

        return self.client.load_table_from_json(
            rows,
            f"{self.DATASET}._stage_{self.TABLE}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=schema,
            ),
        ).result()

    def update(self):
        loader = jinja2.FileSystemLoader(searchpath="./")
        env = jinja2.Environment(loader=loader)
        template = env.get_template("update.sql.j2")
        rendered_query = template.render(dataset=self.DATASET, table=self.TABLE)
        _ = self.client.query(rendered_query).result()

    def run(self):
        rows = self.fetch()
        rows = self.transform(rows)
        results = self.load(rows)
        _ = self.update()
        return {
            "table": f"{self.DATASET}.{self.TABLE}",
            "start_date": self.start_date,
            "end_date": self.end_date,
            "num_processed": self.num_processed,
            "output_rows": results.output_rows,
            "errors": results.errors,
        }


def main(event, context):
    data = event["data"]
    message = json.loads(base64.b64decode(data).decode("utf-8"))
    print(message)
    if "start_date" in message and "end_date" in message:
        job = ShopifyOrdersJob(start_date=message["start_date"], end_date=message["end_date"])
    else:
        job = ShopifyOrdersJob()

    response = {"pipelines": "Shopify Orders", "results": job.run()}
    print(response)
    return response
