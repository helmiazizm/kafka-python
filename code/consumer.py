from kafka import KafkaConsumer
from os import path
import sys, json
import time
import pandas as pd

def server_list(host, ports):
    if ',' in ports:
        ports = list(ports.split(","))
        servers = [host + ":" + port for port in ports]
    else:
        servers = f'{host}:{ports}'
    return servers

def consumer_func(topic, host, port, size, file_csv):
    locate_csv = f"../data/consume/{file_csv}"
    servers = server_list(host, port)
    # consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9093', 'localhost:9094', 'localhost:9095'])
    consumer = KafkaConsumer(topic, bootstrap_servers=servers)
    for i, message in enumerate(consumer):
        data = message.value.decode("utf-8")
        data = json.loads(data)
        df = pd.DataFrame.from_records([data])
        if i == 0 and not path.exists(locate_csv):
            df.to_csv(locate_csv, mode='a', index=False, header=True)
        else:
            df.to_csv(locate_csv, mode='a', index=False, header=False)
        if i % int(size) == 0:
            print('written ' + str(i+1) + ' messages to topic '+ topic)
            # time.sleep(1)
    

if __name__ == "__main__":
    # consumer_func(sys.argv[1], sys.argv[2], sys.argv[3])
    consumer_func(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])