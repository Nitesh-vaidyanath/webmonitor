# webmonitor

This is used for monitoring your website and also persist results on postgress sql. Later results could be aggregated from postgress sql.

web_monitor.py gets response code, response time, error message and text search; final results are sent to kafka brokers. This script could be run as cronjob or kuberentes job. 
Command line arguments or ini config file can be used, currently envirnoment vairbales are not supported.

Note: Even for certificates envirnoment vairables can be added in config file but currently it is not supported.

> python3 webmon/web_monitor.py  --config_file  /tmp/test.ini  --kafka_ca ./certs/ca.crt --kafka_certfile ./certs/client.crt --kafka_keyfile ./certs/client.key
```
python3 web_monitor.py --help
usage: web_monitor.py [-h] [--config_file CONFIG_FILE] [--urls URLS [URLS ...]] [--kafka_brokers KAFKA_BROKERS] [--regex REGEXSTRING] [--topic TOPIC] [--kafka_ssl KAFKA_SSL]
                      [--kafka_ca KAFKA_CA] [--kafka_certfile KAFKA_CERTFILE] [--kafka_keyfile KAFKA_KEYFILE]

Web monitor

optional arguments:
  -h, --help            show this help message and exit
  --config_file CONFIG_FILE
                        config file
  --urls URLS [URLS ...]
                        Website url to monitor. List of urls
  --kafka_brokers KAFKA_BROKERS
                        Website url to monitor
  --regex REGEXSTRING   regex match on text
  --topic TOPIC         kafka topic name
  --kafka_ssl KAFKA_SSL
                        ssl enabled
  --kafka_ca KAFKA_CA   kafka ca file
  --kafka_certfile KAFKA_CERTFILE
                        kafka client cert for mtls
  --kafka_keyfile KAFKA_KEYFILE
                        kafka client ket for mtls
```

Config ini file
```
[kafkaBrokers]
brokers=kafka.<>.aivencloud.com:<port>
topic=test
kafka_group_id=test-consumer-group
kafka_ssl=True
kafka_ca=/home/ubuntu/certs/ca.crt
kafka_certfile=/home/ubuntu/certs/client.crt
kafka_keyfile=/home/ubuntu/certs/client.key

[urls]
urls=http://www.testingmcafeesites.com/index.html2,http://python-requests.org/x.txt,http://www.testingmcafeesites.com/index.html,http://www.testingmcafeesites.com/index.html,http://testnoavailable.com

[postgress]
ps_host=pg-<>.aivencloud.com
ps_port=<port>
ps_user=<admin>
ps_password=<pass>
ps_db=<db>
ps_ca=/home/ubuntu/certs/ps_ca.pem
```

Run consumer script to cosume messages from kafka brokers and write results to postgres sql.
> python3 webmon/persist_result.py  --config_file  /tmp/test.ini  --kafka_ca ./certs/ca.crt --kafka_certfile ./certs/client.crt --kafka_keyfile ./certs/client.key --ps_ca ./certs/ps_ca.pem
```
Web monitor

optional arguments:
  -h, --help            show this help message and exit
  --config_file CONFIG_FILE
                        config file
  --kafka_brokers KAFKA_BROKERS
                        Website url to monitor
  --topic TOPIC         kafka topic name
  --kafka_ssl KAFKA_SSL
                        ssl enabled
  --kafka_ca KAFKA_CA   kafka ca file
  --kafka_certfile KAFKA_CERTFILE
                        kafka client cert for mtls
  --kafka_keyfile KAFKA_KEYFILE
                        kafka client ket for mtls
  --kafka_group_id KAFKA_GROUP_ID
                        consumer group id
  --ps_host PS_HOST     postgress sql host
  --ps_port PS_PORT     postgress sql port
  --ps_user PS_USER     postgress sql user
  --ps_password PS_PASSWORD
                        postgress sql password
  --ps_db PS_DB         postgress sql db
  --ps_ssl PS_SSL       postgress ssl enabled
  --ps_ca PS_CA         postgress ca cert
```

Test cases:
```
python3 tests/test_web_monitor.py 
2021-04-26 22:20:59,446 - INFO - get details for url http://test.com
.2021-04-26 22:20:59,448 - INFO - get details for url http://test.com
.
----------------------------------------------------------------------
Ran 2 tests in 0.015s

OK
```
```
python3 tests/test_modules.py
/home/ubuntu/webmon/web_monitor.py:6: MonkeyPatchWarning: Monkey-patching ssl after ssl has already been imported may lead to errors, including RecursionError on Python 3.6. It may also silently lead to incorrect behaviour on Python 3.7. Please monkey-patch earlier. See https://github.com/gevent/gevent/issues/1016. Modules that had direct imports (NOT patched): ['urllib3.contrib.pyopenssl (/usr/lib/python3/dist-packages/urllib3/contrib/pyopenssl.py)'].
  monkey.patch_all(thread=False, socket=False)
2021-04-27 00:46:08,946 - INFO - commited to postgress database.table : webstatus.webmonitor table
2021-04-27 00:46:08,946 - INFO - commited to postgress database.table : webstatus.webmonitor table
INFO:Webmonitor:commited to postgress database.table : webstatus.webmonitor table
2021-04-27 00:46:09,073 - INFO - commited to postgress database.table : webstatus.webmonitor table
2021-04-27 00:46:09,073 - INFO - commited to postgress database.table : webstatus.webmonitor table
INFO:Webmonitor:commited to postgress database.table : webstatus.webmonitor table
2021-04-27 00:46:09,199 - INFO - commited to postgress database.table : webstatus.webmonitor table
2021-04-27 00:46:09,199 - INFO - commited to postgress database.table : webstatus.webmonitor table
INFO:Webmonitor:commited to postgress database.table : webstatus.webmonitor table
2021-04-27 00:46:09,326 - INFO - commited to postgress database.table : webstatus.webmonitor table
2021-04-27 00:46:09,326 - INFO - commited to postgress database.table : webstatus.webmonitor table
```

