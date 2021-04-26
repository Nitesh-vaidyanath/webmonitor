import gevent
from gevent import monkey

monkey.patch_all()

import requests
import argparse
import sys
import logging
from configparser import ConfigParser
import json
import os
import re
from kafka import KafkaProducer



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
    parser.add_argument('--urls', dest='urls' , nargs='+', help='Website url to monitor', default=[])
    parser.add_argument('--kafka_brokers', dest='kafka_brokers', type=str, help='Website url to monitor', default="")
    parser.add_argument('--regex', dest='regexString', type=str, help='regex match on text', default="")
    parser.add_argument('--topic', dest='topic', type=str, help='kafka topic name', default="")
    parser.add_argument('--kafka_ssl', dest='kafka_ssl', type=str, help='ssl enabled', default=True)
    parser.add_argument('--kafka_ca', dest='kafka_ca', type=str, help='kafka ca file', default="")
    parser.add_argument('--kafka_certfile', dest='kafka_certfile', type=str, help='kafka client cert for mtls', default="")
    parser.add_argument('--kafka_keyfile', dest='kafka_keyfile', type=str, help='kafka client ket for mtls', default="")
    args = parser.parse_args()
    if args.config_file == "" and ( args.urls == "" or args.kafka_brokers == ""):
        logger.error("Please pass config file or expected command line arguments")
        sys.exit("Please pass config file or expected command line arguments \
                    \nexample: python web_monitor.py -f /etc/config.ini or \
                    \n         python --url \"http://test.com\"  --kafka_brokers <bootstrapserver/brokers> ")
    if args.config_file != "" :
        logger.info("Reading config from {} file".format(args.config_file))
        config_parser = ConfigParser()
        config_parser.read(args.config_file)
        kafka_brokers = config_parser.get('kafkaBrokers', 'brokers')
        kafka_topic = config_parser.get('kafkaBrokers', 'topic')
        urls = config_parser.get('urls', 'urls').split(",")
    if len(args.urls) > 0:
        urls = args.urls
    if args.kafka_brokers != "":
        kafka_brokers = args.kafka_brokers
    if args.kafka_brokers != "":
        kafka_topic = args.topic
    if args.regexString != "":
        regexString = args.regexString
    else: 
        regexString = ""
    if args.kafka_ssl and (args.kafka_certfile == "" or  args.kafka_ca == "" or args.kafka_keyfile == ""):
        logger.error("Need to pass ca cert and also client certificate and key if mtls enabled on server")
        sys.exit(1)
    else:
        kafka_certfile = args.kafka_certfile
        kafka_ca = args.kafka_ca 
        kafka_keyfile = args.kafka_keyfile

    return (kafka_brokers, urls, regexString, args.kafka_ssl, kafka_topic, kafka_ca, kafka_certfile, kafka_keyfile)

class WebSiteMonitor:
    def __init__(self, urls, regex):
        self.urls = urls
        self.regex = regex

    def httpGet(self, url):
        result_dict = dict()
        logger.info("get details for url {}".format(url))
        try:
            result = requests.get(url)
            result_dict.update({"response_code": result.status_code})
            result_dict.update({"response_time": result.elapsed.total_seconds()})
            if self.regex != "":
                match = re.search(self.regex, result.text)
                if match:
                    result_dict.update({"regex_matched": self.regex })
                else: 
                    result_dict.update({"regex_matched": ""})
            else: 
                    result_dict.update({"regex_matched": ""})
            result_dict.update({"message": "success"})
            result_dict.update({"url": url})
            result.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.error(e)
            result_dict.update({"url": url})
            result_dict.update({"message": "Failed to establish a new connection.  Name or service not known" })
            result_dict.update({"response_code": result.status_code})
            result_dict.update({"response_time": result.elapsed.total_seconds()})
            result_dict.update({"regex_matched":  self.regex}) 
        except Exception as e:
            logging.error(e)
            result_dict.update({"url": url})
            result_dict.update({"message": str(e)})
            result_dict.update({"response_code": ""})
            result_dict.update({"response_time": ""})
            result_dict.update({"regex_matched": ""})
        # print(result_dict)
        return (result_dict)

    def asyncCall(self): 
        threads = [gevent.spawn(self.httpGet, _url) for _url in self.urls]
        gevent.joinall(threads)
        return ([thread.value for thread in threads])


class kafkaProducer:
    def __init__(self, kafka_borkers, message, ssl, kafka_ca, kafka_certfile, kafka_keyfile):
        self.kafka_borkers =  kafka_borkers
        if ssl:
            self.producer = KafkaProducer(bootstrap_servers=kafka_borkers,
                                          security_protocol="SSL",
                                          ssl_cafile=kafka_ca,
                                          ssl_certfile=kafka_certfile,
                                          ssl_keyfile=kafka_keyfile,
                                          value_serializer=lambda v: json.dumps(v).encode('ascii'),
                                          key_serializer=lambda v: json.dumps(v).encode('ascii')
                                        )
        else:
            self.producer = KafkaProducer(bootstrap_servers=kafka_borkers,
                                          value_serializer=lambda v: json.dumps(v).encode('ascii'),
                                          key_serializer=lambda v: json.dumps(v).encode('ascii')
                                        )       
        self.messages = message

    @staticmethod
    def handle_success(record_metadata):
        logger.info(record_metadata.topic)
        logger.debug(record_metadata.partition)
        logger.debug(record_metadata.offset)
    
    @staticmethod
    def handle_failure(excp):
        logger.error('I am an errback', exc_info=excp)
    

    def send(self, topic, key=None):
        for message in self.messages:
            logger.debug("sending message {} to kafka".format(message))
            future = self.producer.send(topic, key=key.encode() if key else None,
                                        value=message if message else None).add_callback(kafkaObject.handle_success).add_errback(kafkaObject.handle_failure)


    def finalize(self):
        logger.info("flushing the messages")
        self.producer.flush(timeout=5)
        logger.info("flushing finished")


if __name__ == "__main__":
   kafka_brokers, urls, regexString, kafkaSSL, kafkatopic,  kafka_ca, kafka_certfile, kafka_keyfile = arguments()
   monitorObj = WebSiteMonitor(urls, regexString)
   httpRes = monitorObj.asyncCall()
   kafkaObject = kafkaProducer(kafka_brokers, httpRes, kafkaSSL, kafka_ca, kafka_certfile, kafka_keyfile)
   kafkaObject.send(kafkatopic)
   kafkaObject.finalize()   



   
      



