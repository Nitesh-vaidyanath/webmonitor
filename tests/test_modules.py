from configparser import ConfigParser
import os
import sys
testdir = os.path.dirname(__file__)
srcdir = '../webmon'
sys.path.insert(0, os.path.abspath(os.path.join(testdir, srcdir)))

import persist_result
import web_monitor
import unittest

web_monitor.monkey.patch_all(thread=False, socket=False)

class DBTest(unittest.TestCase):
    def setUp(self):
        config_parser = ConfigParser()
        config_path = os.environ['WEB_CONFIG_FILE']
        config_parser.read(config_path)

        self.kafka_brokers = config_parser.get('kafkaBrokers', 'brokers')
        self.kafka_topic = config_parser.get('kafkaBrokers', 'topic')
        self.kafka_group_id = config_parser.get('kafkaBrokers', 'kafka_group_id')
        self.kafka_ca = config_parser.get('kafkaBrokers', 'kafka_ca')
        self.kafka_certfile = config_parser.get('kafkaBrokers', 'kafka_certfile')
        self.kafka_keyfile = config_parser.get('kafkaBrokers', 'kafka_keyfile')
        self.kafka_ssl = config_parser.get('kafkaBrokers', 'kafka_ssl')

        self.ps_host = config_parser.get('postgress', 'ps_host')
        self.ps_port = config_parser.get('postgress', 'ps_port')
        self.ps_user = config_parser.get('postgress', 'ps_user')
        self.ps_password = config_parser.get('postgress', 'ps_password')
        self.ps_db = config_parser.get('postgress', 'ps_db')
        self.ps_ca = config_parser.get('postgress', 'ps_ca')

        self.test_result = {
            "response_code" : "200",
            "response_time" : "0.334216",
            "regex_matched" : "test",
            "message"       : "success",
            "url"           : "http://www.testingmcafeesites.com/index.html2"
        }

    def test_kafkaproducer(self):
        kafkaObject = web_monitor.kafkaProducer(self.kafka_brokers, self.test_result, True, self.kafka_ca, self.kafka_certfile, self.kafka_keyfile)
        kafkaObject.send(self.kafka_topic)
        kafkaObject.finalize()

    def test_kafkaconsumer_postgress(self):
        kafkaObj = persist_result.kafkaConsumer(self.kafka_brokers, True, self.kafka_topic,  self.kafka_ca, self.kafka_certfile, self.kafka_keyfile ,
                                                self.kafka_group_id, self.ps_host, self.ps_port, self.ps_user, self.ps_password, self.ps_db, True, self.ps_ca )
        kafkaObj.saveMessages()

if __name__ == "__main__":
   unittest.main()
