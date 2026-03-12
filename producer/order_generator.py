import json
import time
import random
from faker import Faker
from google.cloud import pubsub_v1

project_id = "meli-data-platform"
topic_id = "meli-orders-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

fake = Faker()

categories = [
    "electronics",
    "home",
    "sports",
    "fashion",
    "toys"
]

def generate_order():

    order = {
        "order_id": fake.uuid4(),
        "user_id": random.randint(1000, 9999),
        "product_id": random.randint(100, 999),
        "category": random.choice(categories),
        "price": round(random.uniform(10, 500), 2),
        "quantity": random.randint(1, 3),
        "timestamp": fake.iso8601()
    }

    return order


while True:

    # generar entre 5 y 10 órdenes por segundo
    batch_size = random.randint(5, 10)

    for _ in range(batch_size):

        order = generate_order()

        data = json.dumps(order).encode("utf-8")

        publisher.publish(topic_path, data)

        print("Sent order:", order["category"], order["price"])

    # esperar 2 segundos antes del siguiente batch
    time.sleep(2)