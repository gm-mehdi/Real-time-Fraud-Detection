from kafka import KafkaConsumer
import json

topic = 'transactions'

consumer = KafkaConsumer(topic,
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

try:
    for message in consumer:
        transaction = message.value
        print("Received:", transaction)
except KeyboardInterrupt:
    consumer.close()