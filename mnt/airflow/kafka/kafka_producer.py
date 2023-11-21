from kafka import KafkaProducer
import json
import time
import random

def generate_data():
    return {'value': random.randint(1, 100)}

def produce_data(producer, topic):
    for i in range(8):
        data = generate_data()
        producer.send(topic, json.dumps(data).encode('utf-8'))
        time.sleep(1)

if __name__ == "__main__":
    kafka_topic = "example_data"
    kafka_bootstrap_servers = "kafka:9092"
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

    produce_data(producer, kafka_topic)
