from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

config_parser = ConfigParser()
config_parser.read_file(args.config_file)
config = dict(config_parser['default'])
config.update(config_parser['consumer'])
config.update(config_parser['schema'])
topic= config_parser['topic']['topic']


c = AvroConsumer(config)

c.subscribe([topic])

while True:
    try:
        msg = c.poll(0)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue

    print(msg.value())

c.close()
