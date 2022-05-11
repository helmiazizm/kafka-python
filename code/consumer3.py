from kafka import KafkaConsumer
from os import path
import sys, json
import time
import pandas as pd

def consumer_func(topic, server, size, file_csv):
    locate_csv = f"../data/consume/${file_csv}3"
    consumer = KafkaConsumer(topic, bootstrap_servers=server)
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
    consumer_func(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])