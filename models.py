import os
import json
from datetime import datetime, timedelta

import requests
import jinja2
from google.cloud import bigquery

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

BQ_CLIENT = bigquery.Client()

API_VER = os.getenv('API_VER')

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)

class Orders:
    table = "Orders"

    def __init__(self, client_name, start, end):
        """Inititate job"""   

        self.client_name = client_name 
        self.api_key, self.api_secret, self.shop_url = self.get_credentials(client_name)
        # self.api_secret = api_secret 
        # self.shop_url = shop_url
        self.dataset = f"{client_name}_Shopify"
        self.start, self.end = self.get_time_range(start, end)

        self.fields, self.encoded_fields, self.schema, self.keys = self.get_config()

    def get_credentials(self, client_name):
        with open('configs/clients.json', 'r') as f:
            clients = json.load(f)
        shop_url = [i['shop_url'] for i in clients if i['client_name']==client_name][0]
        return os.getenv(f"{client_name}_API_KEY"), os.getenv(f"{client_name}_API_SECRET"), shop_url

    def get_time_range(self, _start, _end):
        if _start and _end:
            start, end = _start, _end
        else:
            now = datetime.utcnow()
            end = now.strftime(TIMESTAMP_FORMAT)
            start = (now - timedelta(days=1)).strftime(TIMESTAMP_FORMAT)
        return start, end

    def get_config(self):
        with open("configs/Orders.json", "r") as f:
            config = json.load(f)
        config_fields = config['fields']
        fields = config_fields['fields']
        encoded_fields = config_fields["encoded_fields"]
        schema = config['schema']
        keys = config['keys']
        return fields, encoded_fields, schema, keys

    def get(self):
        """Fetch data from Shopify API

        Returns:
            list: List of results as JSON
        """        

        base_url = f"https://{self.api_key}:{self.api_secret}@{self.shop_url}/admin/api/{API_VER}"
        endpoint = "orders.json"
        url = f"{base_url}/{endpoint}"

        orders = []
        params = {
                "limit": 250,
                "status": "any",
                "updated_at_min": self.start,
                "updated_at_max": self.end,
                "fields": ",".join(self.fields),
            }
        with requests.Session() as session:
            _url = url
            while _url:
                with session.get(_url, params=params) as r:
                    res = r.json()
                orders.extend(res.get("orders"))
                next_link = r.links.get("next")
                if next_link:
                    _url = next_link.get("url")
                    _url = _url.replace(
                        self.shop_url,
                        f"{self.api_key}:{self.api_secret}@{self.shop_url}",
                    )
                else:
                    _url = None
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
                if f in i:
                    i[f] = json.dumps(i[f])
                else:
                    pass
        return rows

    def load(self, rows):
        """Load to BigQuery with predetermined schema to staging table

        Args:
            rows (list): List of results as JSON

        Returns:
            google.cloud.bigquery.job.base_AsyncJob: LoadJob Results
        """

        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{self.dataset}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=self.schema,
            ),
        ).result()

    def update(self):
        """Update the main table using the staging table"""
        
        template = TEMPLATE_ENV.get_template("update.sql.j2")
        rendered_query = template.render(dataset=self.dataset, table=self.table)
        _ = BQ_CLIENT.query(rendered_query)

    def run(self):
        """Run the job

        Returns:
            dict: Job's Results
        """

        rows = self.get()
        responses = {
            "table": f"{self.dataset}.{self.table}",
            "start_date": self.start,
            "end_date": self.end,
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            _ = self.update()
            responses = {
                **responses,
                "num_processed": len(rows),
                "output_rows": loads.output_rows,
            }
        return responses
