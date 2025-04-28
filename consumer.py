from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
import sys
import logging

    


kafka_config = {
    'bootstrap.servers': 'localhost:9092', 
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(kafka_config)
#topics = ['sensor_data']
topic_partition0 = TopicPartition("sensor_data2", 0)
#topic_partition1 = TopicPartition('sensor_data2', 1)

consumer.assign([topic_partition0])
try:
    # подписываемся на топик
    #consumer.subscribe([topic_partition])

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            print(f"Received message: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()