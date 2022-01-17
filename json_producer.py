from argparse import ArgumentParser, FileType
from configparser import ConfigParser
import csv
from uuid import uuid4

from six.moves import input

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(config_parser,filename):
    topic= config_parser['topic']['topic']

    schema_str = """
    {
      "title": "Routes",
      "description": "MAd tiNG",
      "type": "object",
      "properties": {
        "route": {
          "description": "route id",
          "type": "string"
        },
        "step": {
          "description": "step id of the route",
          "type": "string"
        },
        "geom": {
          "description": "wkt geometric rep of the step",
          "type": "string"
        }
      },
      "required": [ "route", "step", "geom" ]
    }
    """
    schema_registry_conf = {'url': config_parser['schema']['schema.registry.url']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    json_serializer = JSONSerializer(schema_str, schema_registry_client)

    producer_conf = {'bootstrap.servers': config_parser['default']['bootstrap.servers'],
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': json_serializer}

    producer = SerializingProducer(producer_conf)

    producer.poll(0.0)
    with open(filename,'r') as read_tsv:
        tsv_file = csv.reader(read_tsv,delimiter="\t")
        next(tsv_file)
        for line in tsv_file:
            value = {"route": line[0],"step":line[1], "geom":line[2]}            
            producer.produce(topic=topic, key=f"R{line[0]}S{line[1]}", value=value,on_delivery=delivery_report)
    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument("--file",help="FQN of the input file eg. routes.tsv",required=True)
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()
    filename = args.file
    config_parser = ConfigParser()    
    config_parser.read_file(args.config_file)
    main(config_parser,filename)