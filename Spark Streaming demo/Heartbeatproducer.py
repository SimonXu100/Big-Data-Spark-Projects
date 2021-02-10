import time
from time import gmtime, strftime
from kafka import KafkaProducer

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print("Message '{0}' published successfully.".format(value))
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def main():
    kafka_producer = connect_kafka_producer()
    messages = 360
    sleepTime = 10
    while (messages > 0):
        messages = messages - 1
        showtime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        publish_message(kafka_producer, 'heartbeat', 'raw', showtime)
        time.sleep(sleepTime)

if __name__ == '__main__':
    main()