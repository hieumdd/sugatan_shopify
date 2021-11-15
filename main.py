from models.shopify import Orders
from controller.handler import run
from controller.tasks import create_tasks


def main(request):
    data = request.get_json()
    print(data)

    if data:
        if "tasks" in data:
            response = create_tasks(data)
        else:
            err_response, response = run(
                Orders,
                data["auth"],
                data.get("start"),
                data.get("end"),
            )
            if err_response:
                raise err_response
    print(response)
    return response
