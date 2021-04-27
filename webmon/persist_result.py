from kafka import KafkaProducer
import requests
import argparse
import sys
import logging
from configparser import ConfigParser
import json
import os
import re
from kafka import KafkaConsumer
import ssl
import ast
from psycopg2.extras import RealDictCursor
import psycopg2

logger = logging.getLogger('Webmonitor')
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

def arguments():
    parser = argparse.ArgumentParser(description='Web monitor')
    parser.add_argument('--config_file', dest='config_file',type=str,help='config file', default="")
    parser.add_argument('--kafka_brokers', dest='kafka_brokers', type=str, help='Website url to monitor', default="")
    parser.add_argument('--topic', dest='topic', type=str, help='kafka topic name', default="")
    parser.add_argument('--kafka_ssl', dest='kafka_ssl', type=str, help='ssl enabled', default=True)
    parser.add_argument('--kafka_ca', dest='kafka_ca', type=str, help='kafka ca file', default="")
    parser.add_argument('--kafka_certfile', dest='kafka_certfile', type=str, help='kafka client cert for mtls', default="")
    parser.add_argument('--kafka_keyfile', dest='kafka_keyfile', type=str, help='kafka client ket for mtls', default="")
    parser.add_argument('--kafka_group_id', dest='kafka_group_id', type=str, help='consumer group id', default="test-consumer-group")
    parser.add_argument('--ps_host', dest='ps_host', type=str, help='postgress sql host', default="")
    parser.add_argument('--ps_port', dest='ps_port', type=int, help='postgress sql port', default=0)
    parser.add_argument('--ps_user', dest='ps_user', type=str, help='postgress sql user', default="")
    parser.add_argument('--ps_password', dest='ps_password', type=str, help='postgress sql password', default="")
    parser.add_argument('--ps_db', dest='ps_db', type=str, help='postgress sql db', default="")
    parser.add_argument('--ps_ssl', dest='ps_ssl', type=str, help='postgress ssl enabled', default=True)
    parser.add_argument('--ps_ca', dest='ps_ca', type=str, help='postgress ca cert', default="")
    args = parser.parse_args()
    if args.config_file == "" and args.kafka_brokers == "":
        logger.error("Please pass config file or expected command line arguments")
        sys.exit("Please pass config file or expected command line arguments \
                    \nexample:  python --kafka_brokers <bootstrapserver/brokers> ")
    if args.config_file != "" :
        logger.info("Reading config from {} file".format(args.config_file))
        config_parser = ConfigParser()
        config_parser.read(args.config_file)
        kafka_brokers = config_parser.get('kafkaBrokers', 'brokers')
        kafka_topic = config_parser.get('kafkaBrokers', 'topic')
        kafka_group_id = config_parser.get('kafkaBrokers', 'kafka_group_id')
        ps_host = config_parser.get('postgress', 'ps_host')
        ps_port = config_parser.get('postgress', 'ps_port')
        ps_user = config_parser.get('postgress', 'ps_user')
        ps_password = config_parser.get('postgress', 'ps_password')
        ps_db = config_parser.get('postgress', 'ps_db')
    if args.kafka_brokers != "":
        kafka_brokers = args.kafka_brokers
    if args.kafka_brokers != "":
        kafka_topic = args.topic
    if args.kafka_group_id != "":
        kafka_group_id = args.kafka_group_id
    if args.kafka_ssl and (args.kafka_certfile == "" or  args.kafka_ca == "" or args.kafka_keyfile == ""):
        logger.error("Need to pass ca cert and also client certificate and key if mtls enabled on server")
        sys.exit(1)
    else:
        kafka_certfile = args.kafka_certfile
        kafka_ca = args.kafka_ca
        kafka_keyfile = args.kafka_keyfile
    if args.ps_host != "":
       ps_host = args.ps_host
    if args.ps_port != 0:
       ps_port = args.ps_port
    if args.ps_user != "" :
       ps_user = args.ps_user
    if args.ps_password!= "":
       ps_password = args.ps_password
    if args.ps_db != "":
       ps_db = args.ps_db
    if args.ps_ssl and args.ps_ca == "":
       logger.error("Need to pass postgress server ca cert")
       sys.exit(1)
    else:
       ps_ca = args.ps_ca
    return (kafka_brokers, args.kafka_ssl, kafka_topic, kafka_ca, kafka_certfile, kafka_keyfile, args.kafka_group_id,
            ps_host, ps_port, ps_user, ps_password, ps_db, args.ps_ssl, ps_ca)


class kafkaConsumer:
    '''This class will consume messages from kafka broker'''
    def __init__(self, kafka_borkers, kafka_ssl, topic, kafka_ca, kafka_certfile, kafka_keyfile, kafka_group_id,
                 ps_host, ps_port, ps_user, ps_password, ps_db, ps_ssl, ps_ca):
        self.kafka_borkers =  kafka_borkers
        postgressObj = postgress(ps_host, ps_port, ps_user, ps_password, ps_db, ps_ssl, ps_ca)
        self.ps_cursor, self.ps_conn = postgressObj.ps_cursor()
        self.ps_db = ps_db
        if kafka_ssl:
            try:
                self.consumer = KafkaConsumer(topic,
                                          group_id=kafka_group_id,
                                          auto_offset_reset="earliest",
                                          bootstrap_servers=kafka_borkers,
                                          security_protocol="SSL",
                                          ssl_cafile=kafka_ca,
                                          ssl_certfile=kafka_certfile,
                                          ssl_keyfile=kafka_keyfile,
                                        )
            except Exception as e:
                logger.error(e)
        else:
            try:
                self.consumer = KafkaConsumer(topic,
                                          group_id=kafka_group_id,
                                          auto_offset_reset="earliest",
                                          bootstrap_servers=kafka_borkers,
                                          value_serializer=lambda v: json.dumps(v).encode('ascii'),
                                          key_serializer=lambda v: json.dumps(v).encode('ascii')
                                        )
            except Exception as e:
                    logger.error(e)

    def saveMessages(self):
        for message in self.consumer:
            logging.debug("%s:%d:%d".format(message.topic, message.partition,message.offset))
            try:
                msg = json.loads(message.value.decode("utf-8").replace("'",'"'))
            except json.decoder.JSONDecodeError:
                logger.error(message)
                self.consumer.commit()
                continue
            self.ps_cursor.execute("""INSERT INTO webmonitor (response_code, response_time, regex_matched, message, url)
                          VALUES (%s, %s, %s, %s, %s)""",(msg["response_code"], msg["response_time"], msg["regex_matched"],
                          msg["message"], msg["url"]))
            self.ps_conn.commit()
            logger.info("commited to postgress database.table : {}.{} table".format(self.ps_db, "webmonitor"))
            self.consumer.commit()

class postgress:
    '''Postgress connection'''
    def __init__(self, ps_host, ps_port, ps_user, ps_password, ps_db, ps_ssl, sslrootcert):
        try:
          if ps_ssl:
                os.environ["PGSSLROOTCERT"] = sslrootcert
                uri = "postgres://{}:{}@{}:{}/{}?sslmode=require".format(ps_user, ps_password, ps_host, ps_port, ps_db)
                self.db_conn = psycopg2.connect(uri)
                self.cursor = self.db_conn.cursor()
          else:
                uri = "postgres://{}:{}@{}:{}/{}?".format(ps_user, ps_password, ps_host, ps_port, ps_db)
                self.db_conn = psycopg2.connect(uri)
                self.cursor = self.db_conn.cursor()
        except Exception as e:
            logger.error(e)
    def ps_cursor(self):
        return self.cursor, self.db_conn


if __name__ == "__main__":
   kafka_brokers, kafkaSSL, kafkatopic,  kafka_ca, kafka_certfile, kafka_keyfile , kafka_group_id, ps_host, ps_port, ps_user, ps_password, ps_db, ps_ssl, ps_ca = arguments()
   kafkaObj = kafkaConsumer(kafka_brokers, kafkaSSL, kafkatopic,  kafka_ca, kafka_certfile, kafka_keyfile , kafka_group_id,
                           ps_host, ps_port, ps_user, ps_password, ps_db, ps_ssl, ps_ca )
   kafkaObj.saveMessages()

