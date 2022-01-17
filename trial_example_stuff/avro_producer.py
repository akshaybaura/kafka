from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
import csv

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

config_parser = ConfigParser()
config_parser.read_file(args.config_file)
config = dict(config_parser['default'])
config.update(config_parser['schema'])
topic= config_parser['topic']['topic']

key_schema_str = """
{
   "namespace": "route.key",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "route",
       "type" : "string"
     }
   ]
}
"""

value_schema_str = """
{
   "namespace": "route.val",
   "name": "value",
   "type": "record",
   "fields" : [
     {"name" : "step","type" : "string"},
     {"name" : "geom","type" : "string"}
   ]
}
"""


value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

avroProducer = AvroProducer(config, default_key_schema=key_schema, default_value_schema=value_schema)

with open('routes.tsv','r') as read_tsv:
        tsv_file = csv.reader(read_tsv,delimiter="\t")
        next(tsv_file)
        for line in tsv_file:
            key = {"route": line[0]}
            value = {"step":line[1], "geom":line[2]}
            avroProducer.produce(topic=topic, value=value, key=key)

avroProducer.flush()
