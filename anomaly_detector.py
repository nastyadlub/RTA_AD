from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
user_history = defaultdict(list)
for message in consumer:
    tx = message.value
    u_id = tx['user_id']
    curr_time = datetime.fromisoformat(tx['timestamp'])
    
    user_history[u_id].append(curr_time)
    user_history[u_id] = [t for t in user_history[u_id] if (curr_time - t).total_seconds() <= 60]
    
    if len(user_history[u_id]) > 3:
        print(f"ANOMALIA! Użytkownik {u_id} zrobił {len(user_history[u_id])}zakupy w 60s!")
