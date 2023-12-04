import sys
import msgpack
import argparse
import csv
import os

from confluent_kafka import Consumer, TopicPartition, KafkaError

def compareTimestamps(topic):
    timestamps = {}

    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'ihr_tail',  # does this need to be named this?
        'enable.auto.commit': False,
    })

    # populate the timestamps dictionary
    while True:
        msg = consumer.poll(1000)
        msgdict = {
            'topic': msg.topic(),
            'partition': msg.partition(),
            'key': msg.key(),
            'timestamp': msg.timestamp(),
            'headers': msg.headers(),
            'value': msgpack.unpackb(msg.value(), raw=False)
        }

        data = msgdict['value']
        # timestamps[data.timestamp] = 1

        i += 1
        if i % 50 == 0:
            print("hello")

        if i >= high or i >= 1000000:
            break

    consumer.close()

    for timestamp in timestamps.keys():
        seconds = int(timestamp) / 1000
        days = seconds / (24 * 60 * 60)
        print(str(days) + " days ago")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--topic', default='ihr_bgp_atom_route-views2')
    args = parser.parse_args()

    compareTimestamps(args.topic)