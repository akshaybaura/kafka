from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.admin import AdminClient, NewTopic
from shapely import wkt
from uuid import uuid4
import time



class Consumer_conf:
  def __init__(self):
      self.schema_str = """
      {
        "title": "Routes",
        "description": "syo-syo-nut",
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
      self.json_deserializer = JSONDeserializer(self.schema_str)
      self.string_deserializer = StringDeserializer('utf_8')

  def get_conf(self,config_parser):
    self.topic= config_parser['topic']['topic']
    adminclient = AdminClient({'bootstrap.servers': config_parser['default']['bootstrap.servers']})
    if self.topic not in adminclient.list_topics(timeout=10).topics.keys():
      new_topic = [NewTopic(self.topic, num_partitions=1, replication_factor=1)]
      fs = adminclient.create_topics(new_topic)
      for topic, f in fs.items():
        print("Topic {} created".format(topic))
      time.sleep(5)
    self.consumer_conf = {'bootstrap.servers': config_parser['default']['bootstrap.servers'],
                   'key.deserializer': self.string_deserializer,
                   'value.deserializer': self.json_deserializer,
                   'group.id': config_parser['consumer']['group.id'],
                   'auto.offset.reset': "earliest"}
    self.consumer = DeserializingConsumer(self.consumer_conf)
    self.consumer.subscribe([self.topic])
    return self.consumer

class Producer_conf:
  def __init__(self,config_parser):
      self.config_parser = config_parser
      self.schema_str = """
      {
        "title": "Routes",
        "description": "syo-syo-nut",
        "type": "object",
        "properties": {
          "route": {
            "description": "route id",
            "type": "integer"
          },
          "step": {
            "description": "step id of the route",
            "type": "integer"
          },
          "geom": {
            "description": "wkt length of linestring",
            "type": "number"
          }
        },
        "required": [ "route", "step", "geom" ]
      }
      """
      self.schema_registry_conf = {'url': config_parser['schema']['schema.registry.url']}
      self.schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)
      self.json_serializer = JSONSerializer(self.schema_str, self.schema_registry_client)

  def get_conf(self):
    self.producer_conf = {'bootstrap.servers': self.config_parser['default']['bootstrap.servers'],
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': self.json_serializer}
    self.producer = SerializingProducer(self.producer_conf)
    return self.producer

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def main(config_parser):
    consumer_obj= Consumer_conf()
    consumer = consumer_obj.get_conf(config_parser)

    producer_obj = Producer_conf(config_parser)
    producer= producer_obj.get_conf()
    reroute_topic= config_parser['topic']['reroute_topic']

    while True:
        try:
            msg = consumer.poll(0)
            if msg is None:
                continue
            key=msg.key()
            route = msg.value()
            if route is not None:
                route.update({'route':int(route['route']),'step':int(route['step']),'geom':wkt.loads(route['geom']).length})
                print(route)
                producer.produce(topic=reroute_topic, key=key, value=route,on_delivery=delivery_report)
                producer.flush()
        except KeyboardInterrupt:
          consumer.close()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    config_parser = ConfigParser()    
    config_parser.read_file(args.config_file)
    main(config_parser)


