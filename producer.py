from faker import Faker
import random
from kafka import KafkaProducer
import json

from model import is_fraud

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
        transactions = generate_transaction()
        # TODO: model training
        if is_fraud(transactions):
            producer.send(topic, value=transactions)
            print("Fraud detected and sent:", transactions)
except KeyboardInterrupt:
    producer.close()