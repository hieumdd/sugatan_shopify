import json
import base64

from models import Orders
from broadcast import broadcast

def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if data:
        if "broadcast" in data:
            job = broadcast()
        else:
            job = Orders(
                data['client_name'],
                data.get('start'),
                data.get('end')
            ).run()
    response = {"pipelines": "Shopify", "results": job}
    print(response)
    return response
