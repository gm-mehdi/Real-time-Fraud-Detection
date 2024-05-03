from faker import Faker
import random
from kafka import KafkaProducer
import json

fake = Faker()

def generate_transaction():
    transaction_type = random.choice(["cash_in", "cash_out", "payment", "transfer"])
    is_fraud = random.choice([True, False])
    return {
        "timestamp": fake.date_time_this_month().strftime("%Y-%m-%d %H:%M:%S"),
        "card_number": fake.credit_card_number(),
        "amount": round(random.uniform(1, 10000), 2),
        "type": transaction_type,
        "isFlaggedFraud": is_fraud
    }

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

topic = 'transactions'

try:
    while True:
        transaction = generate_transaction()
        # TODO: model training
        producer.send(topic, value=transaction)
        print("Sent:", transaction)
except KeyboardInterrupt:
    producer.close()