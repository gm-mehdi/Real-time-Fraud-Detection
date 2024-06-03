from faker import Faker
import random
from kafka import KafkaProducer
import json

import fraudDection


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
        if fraudDection.is_fraud(transactions):
            producer.send(topic, value=transactions)
            print("Fraud detected and sent:", transactions)
except KeyboardInterrupt:
    producer.close()

# from faker import Faker
# import random
# import pandas as pd
# import json

# fake = Faker()

# def generate_transaction():
#     transaction_type = random.choice(["cash_in", "cash_out", "payment", "transfer"])
#     is_fraud = random.choice([1, 0])
#     return {
#         "timestamp": fake.date_time_this_month().strftime("%Y-%m-%d %H:%M:%S"),
#         "card_number": fake.credit_card_number(),
#         "amount": round(random.uniform(1, 10000), 2),
#         "type": transaction_type,
#         "isFlaggedFraud": is_fraud
#     }

# transactions = []

# for _ in range(5000):
#     transaction = generate_transaction()
#     transactions.append(transaction)

# # Convert the list of transactions to a pandas DataFrame
# df = pd.DataFrame(transactions)

# # Save the DataFrame to a CSV file
# df.to_csv('transactions.csv', index=False)

# print("Saved 5000 transactions to 'transactions.csv'")
