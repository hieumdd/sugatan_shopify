import json
import base64
from datetime import datetime

import requests

data = {
    "message": {
        "data": base64.b64encode(
            json.dumps(
                {
                    "start_date": datetime(2021, 3, 1).strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "end_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
                }
            ).encode("utf-8")
        ).decode("utf-8")
    }
}
r = requests.get("http://localhost:8080", json=data)
