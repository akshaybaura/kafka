#!/usr/bin/env python

from os import read
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    topic = config_parser['topic']['topic']

    producer = Producer(config)


    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


    with open('routes.tsv','r') as read_tsv:
        Lines=[]
        for line in read_tsv:
            l=line.split('\t')
            Lines.append(l)

    # while Lines:
    #     print(line)
    #     break

    count = 0
    while count<5:
        producer.produce(topic, line, str(count), callback=delivery_callback)
        count += 1

    producer.poll(10000)
    producer.flush()