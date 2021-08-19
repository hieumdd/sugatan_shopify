import os
import json

from google.cloud import pubsub_v1

def get_clients():
    with open('configs/clients.json', 'r') as f:
        return json.load(f)

def broadcast():
    clients = get_clients()
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))

    for client in clients:
        data = client
        message_json = json.dumps(data)
        print(message_json)
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()

    return {
        "message_sent": len(clients),
    }
