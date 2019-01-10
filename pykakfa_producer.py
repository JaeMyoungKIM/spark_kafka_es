#!/usr/bin/env python
from kafka import KafkaConsumer, KafkaProducer
#import logging
#logging.basicConfig(level=logging.DEBUG)

producer = KafkaProducer(bootstrap_servers='localhost:9092')

i = 0
#fd = open('/root/spark-2.1.0-bin-hadoop2.7/tdata/attack.log', 'r')
#fd = open('/var/log/syslog', 'r')
fd = open('./access.log', 'r')
while True:
    line = fd.readline()
    i = i+1
    if i > 10:
        break
    producer.send('jmkim', str(line).encode('utf-8'))
    producer.flush()

fd.close()
