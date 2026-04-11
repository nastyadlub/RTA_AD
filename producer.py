from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
def generate_transaction(i):
    stores = ["Warszawa", "Kraków", "Gdańsk", "Wrocław"]
    categories = ["elektronika", "odzież", "żywność", "książki"]
    return {
        "tx_id": f"TX{i:04d}",
        "user_id": f"u{random.randint(1, 20):02d}",
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(stores),
        "category": random.choice(categories),
        "timestamp": datetime.now().isoformat()
    }
i = 1
try:
    while True:
        tx = generate_transaction(i)
        producer.send('transactions', value=tx)
        print(f"Wysłano: {tx}")
        i += 1
        time.sleep(1)
except KeyboardInterrupt:
    print("Zatrzymano")
