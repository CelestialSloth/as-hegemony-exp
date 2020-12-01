import logging
import json

import msgpack
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic, Producer


with open("config.json", "r") as f:
    config = json.load(f)
KAFKA_BOOTSTRAP_SERVERS = config["kafka"]["bootstrap_servers"]
NO_NEW_MESSAGE_LIMIT = config["kafka"]["no_new_message_limit"]
DEFAULT_TOPIC_CONFIG = config["kafka"]["default_topic_config"]


def create_consumer_and_set_offset(topic: str, timestamp: int):
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'ashege_consumer',
        'session.timeout.ms': 600000,
        'max.poll.interval.ms': 600000,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'
    })
    set_consumer_time_offset(consumer, topic, timestamp)
    return consumer


def set_consumer_time_offset(consumer: Consumer, topic: str, timestamp: int):
    timestamp_ms = timestamp * 1000
    time_offset = consumer.offsets_for_times(
        [TopicPartition(
            topic,
            partition=0,
            offset=timestamp_ms
        )], timeout=1)

    if time_offset == -1:
        raise Exception("cannot assign topic partition")
    consumer.assign(time_offset)


def consume_stream(consumer: Consumer):
    number_of_empty_message = 0

    while True:
        kafka_msg = consumer.poll(1.0)

        if kafka_msg is None:
            number_of_empty_message += 1
            if number_of_empty_message > NO_NEW_MESSAGE_LIMIT:
                return
            continue

        if kafka_msg.error():
            logging.error(f"consumer error {kafka_msg.error()}")
            continue

        message = msgpack.unpackb(kafka_msg.value(), raw=False)
        yield message, kafka_msg


def create_topic(topic_name: str, topic_config=DEFAULT_TOPIC_CONFIG):
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    topic_list = [NewTopic(topic_name, **topic_config)]
    created_topic = admin_client.create_topics(topic_list)

    for topic, future in created_topic.items():
        try:
            future.result()
            logging.warning("Topic {} created".format(topic))
        except Exception as e:
            logging.warning("Failed to create topic {}: {}".format(topic, e))


def prepare_producer():
    logging.debug("prepare producer")
    producer = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'default.topic.config': {
            'compression.codec': 'snappy'
        }
    })
    return producer