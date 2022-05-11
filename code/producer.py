from kafka import KafkaProducer
import pandas as pd
import json, sys, time

def server_list(host, ports):
    if ',' in ports:
        ports = list(ports.split(","))
        servers = [host + ":" + port for port in ports]
    else:
        servers = f'{host}:{ports}'
    return servers

def producer_func(topic, host, ports, size, file_csv = '../data/produce/suicide.csv'):
    server = server_list(host, ports)
    producer = KafkaProducer(bootstrap_servers=server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    data = pd.read_csv(file_csv)
    data = data.to_json(orient='records')
    data = json.loads(data)
    for i, rows in enumerate(data):
        producer.send(topic, rows)
        if i % int(size) == 0:
            print('produced ' + str(i+1) + ' messages to topic '+ topic)
            # time.sleep(1)

if __name__ == "__main__":
    producer_func(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])