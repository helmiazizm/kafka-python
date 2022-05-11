import time
from kafka import KafkaProducer
# from kafka.errors import KafkaError, kafka_errors
# import json
# import traceback
import sys

"""unlock to watch the detailed debug log"""
# import logging
# logging.basicConfig(level=logging.DEBUG)
"""unlock to watch the detailed debug log"""

class Kafka_producer():
     def __init__(self, kafkahost, kafkaport, username, password,
                  key_serializer=None,
                  value_serializer=None,
                  security_protocol="SASL_PLAINTEXT",
                  sasl_mechanism="PLAIN"):
         """
         Parameters
         ----------
         kafkahost : ip
         kafkaport : port
         username ：kafka sasl authentication user name
         password ：kafka sasl authentication password
         The other four parameters do not need to be passed in. Just
take the default value
         """
         self.kafkaHost = kafkahost
         self.kafkaPort = kafkaport
         self.username = username
         self.password = password
         self.ks = key_serializer
         self.vs = value_serializer
         self.sp = security_protocol
         self.sm = sasl_mechanism

         self.producer = KafkaProducer(bootstrap_servers='{kafka_host}:{kafka_port}'.format(
             kafka_host=self.kafkaHost,
             kafka_port=self.kafkaPort),
             sasl_plain_username=self.username,
             sasl_plain_password=self.password,
             key_serializer=self.ks,
             value_serializer=self.vs,
             security_protocol=self.sp,
             sasl_mechanism=self.sm,
             api_version=(0, 10)
         )

     def close(self):
         self.producer.close()

     def sendData(self, topic, path, size):
         """
         Parameters
         ----------
         topic : kafka topic
         path : Local file path of the data you want to send
         size : Every time you send 'size' amount of data,
         take a second off to prevent too much data from being
overstocked in memory
         """
         with open(path, 'r') as f:
             sf = f.read().split('\n')
             l = list(str(s) for s in sf)
         # print(l[0:10])
         # print(len(l))
         # print(l[19999])

         for i in range(1, len(l) + 1):
             self.producer.send(topic, l[i - 1].strip().encode('utf-8'))
             if i % int(size) == 0:
                 print('produced ' + str(i) + ' messages to topic '+topic)
                 time.sleep(1)

         if len(l) % int(size) != 0:
             print('produced ' + str(len(l)) + ' messages to topic '+topic)


if __name__ == '__main__':
     producer = Kafka_producer(sys.argv[1], sys.argv[2], sys.argv[3],sys.argv[4])
     producer.sendData(sys.argv[5], sys.argv[6], sys.argv[7])
     producer.close()