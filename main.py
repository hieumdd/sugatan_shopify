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
        """Inititate job"""   

        self.API_KEY = os.getenv("API_KEY")
        self.API_SECRET = os.getenv("API_SECRET")
        self.API_VER = os.getenv("API_VER")
        self.SHOP_URL = os.getenv("SHOP_URL")

        BUSINESS = os.getenv("BUSINESS")
        self.DATASET = f"{BUSINESS}_Shopify"

        self.TABLE = "Orders"

        # Inititate BigQuery Client
        self.client = bigquery.Client()

        # Inititate time range for the job, defaults to last 30 days
        now = datetime.now()
        self.end_date = kwargs.get("end_date", now.strftime(TIMESTAMP_FORMAT))
        self.start_date = kwargs.get(
            "start_date", (now - timedelta(days=5)).strftime(TIMESTAMP_FORMAT)
        )

        # Inititate the fields to fetch
        with open("config.json", "r") as f:
            config = json.load(f)
        self.fields = config.get("fields")
        self.encoded_fields = config.get("encoded_fields")

    def fetch(self):
        """Fetch data from Shopify API

        Returns:
            list: List of results as JSON
        """        

        # Initial URL to fetch
        INITIAL_URL = f"https://{self.API_KEY}:{self.API_SECRET}@{self.SHOP_URL}/admin/api/{self.API_VER}/orders.json"

        orders = []
        with requests.Session() as session:
            # Setting up params for fetching
            params = {
                "limit": 250,
                "status": "any",
                "updated_at_min": self.start_date,
                "updated_at_max": self.end_date,
                "fields": ",".join(self.fields),
            }
            url = INITIAL_URL

            # Looping to get all results
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
                    else:
                        url = None
                params = {"limit": 250}

        return orders

    def transform(self, rows):
        """Transform results by dumping nested fields as string

        Args:
            rows (list): List of results as JSON

        Returns:
            list: List of results as JSON
        """

        for i in rows:
            for f in self.encoded_fields:
                i[f] = json.dumps(i[f])
        self.num_processed = len(rows)
        return rows

    def load(self, rows):
        """Load to BigQuery with predetermined schema to staging table

        Args:
            rows (list): List of results as JSON

        Returns:
            google.cloud.bigquery.job.base_AsyncJob: LoadJob Results
        """

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
        """Update the main table using the staging table"""    

        loader = jinja2.FileSystemLoader(searchpath="./")
        env = jinja2.Environment(loader=loader)
        
        # Fetch the Update template
        template = env.get_template("update.sql.j2")
        rendered_query = template.render(dataset=self.DATASET, table=self.TABLE)
        _ = self.client.query(rendered_query).result()

    def run(self):
        """Run the job

        Returns:
            dict: Job's Results
        """

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
    """Main function as gateway

    Args:
        event (dict): PubSubMessage
        context (google.cloud.functions.Context): Event Context

    Returns:
        dict: Pipelines results
    """
    # Parse Message data
    data = event["data"]
    message = json.loads(base64.b64decode(data).decode("utf-8"))
    print(message)

    # Inititate Job based on message
    if "start_date" in message and "end_date" in message:
        job = ShopifyOrdersJob(start_date=message["start_date"], end_date=message["end_date"])
    else:
        job = ShopifyOrdersJob()

    response = {"pipelines": "Shopify Orders", "results": job.run()}
    print(response)
    return response
