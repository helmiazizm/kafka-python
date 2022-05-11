from kafka import KafkaConsumer
from os import path
import sys, json
import time
import pandas as pd

def define_datas():
    if path.exists('test3.csv'):
        df = pd.read_csv('test3.csv')
        df = df.to_json(orient='records')
        datas = json.loads(df)
    else:
        datas = []
    return datas

def consumer_func(topic, server, size):
    datas = define_datas()
    consumer = KafkaConsumer(topic, bootstrap_servers=server)
    for i, message in enumerate(consumer):
        data = message.value.decode("utf-8")
        data = json.loads(data)
        df = pd.DataFrame.from_records([data])
        if i == 0 and not path.exists('test3.csv'):
            df.to_csv('test3.csv', mode='a', index=False, header=True)
        else:
            df.to_csv('test3.csv', mode='a', index=False, header=False)
        if i % int(size) == 0:
            print('written ' + str(i+1) + ' messages to topic '+ topic)
            # time.sleep(1)
    

if __name__ == "__main__":
    consumer_func(sys.argv[1], sys.argv[2], sys.argv[3])