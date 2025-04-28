import json
import random
import time
from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}

producer = Producer(config)
def generate_data(message, partition_id):
    return {
        'consumer_id': 1,
        'message': message,
        'partition_id' : partition_id
    }

def serialize_data(data):
    return json.dumps(data)


def send_message(topic, data, partition_id):
    producer.produce(topic,key='key1', value=data, partition=partition_id)
    producer.produce(topic,key='key2', value=data, partition=partition_id)
    producer.flush()

try:
    while True:
        message = str(input())
        data = generate_data(message, partition_id=0)
        data2 = generate_data(message, partition_id=1)

        serialized_data = serialize_data(data)
        serialized_data2 = serialize_data(data2)

        send_message('sensor_data2', serialized_data, 0)
        send_message('sensor_data2', serialized_data2, 1)

        print(f'Sent data: {serialized_data}')
        print(f'Sent data: {serialized_data2}')

        time.sleep(10)
except KeyboardInterrupt:
    print('Stopped.')

producer.close()