from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx['store']
    store_counts[store] += 1
    msg_count += 1
    
    if msg_count % 10 == 0:
        print(f"\nStatystyki (10 msg): {dict(store_counts)}")
