import json
from confluent_kafka import Consumer, KafkaError

def consume_from_kafka(topic_name, group_id, bootstrap_servers='localhost:9092'):
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # You can change this to 'latest' or 'none' based on your requirements
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([topic_name])
    try:
        while True:
            msg = consumer.poll(timeout=1000)  # Adjust the timeout as needed

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            try:
                json_data = json.loads(msg.value().decode('utf-8'))
                print('Received JSON message:', json_data)
            except json.JSONDecodeError as e:
                print('Error decoding JSON:', e)

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

if __name__ == '__main__':
    kafka_topic_name = 'first'
    consumer_group_id = 'grp1'
    consume_from_kafka(kafka_topic_name, consumer_group_id)
