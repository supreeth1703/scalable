from kafka import KafkaProducer
import json
import time

BROKER = 'localhost:9092'
TOPIC = 'arxiv-stream'

def produce_messages():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        linger_ms=10,  # wait up to 10ms to batch messages
        batch_size=32768,  # 32KB batches for higher throughput
        compression_type='gzip',  # compress messages
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("Starting to produce messages to Kafka...")

    with open('/home/ec2-user/environment/arxiv-metadata-oai-snapshot.json') as f:
        for idx, line in enumerate(f, 1):
            record = json.loads(line)
            producer.send(TOPIC, value=record)

            if idx % 100000 == 0:
                print(f"Produced {idx} records so far...")

    producer.flush()
    print(" Done producing data to Kafka.")

if __name__ == "__main__":
    produce_messages()
