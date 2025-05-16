import json
from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

if __name__ == '__main__':
    for i in range(100):
        msg = {'id': i, 'value': i * 2, 'ts': time.time()}
        producer.send('demo-topic', msg)
        time.sleep(0.5)
