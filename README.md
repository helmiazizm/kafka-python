The programs in this repository can be used to stream your desired .csv file into your Kafka broker from the producer and receive it as the same .csv file from the consumer. 

How to run:

1. Clone this repository into your local
2. Make sure that your Kafka brokers and Zookeeper are running
3. Go to code folder by typing `cd code` onto your terminal
4. Run the following to activate your producer:

`python3 producer.py {topic name} {host name} {ports name} {size}`

As for the consumer:

`python3 consumer.py {topic name} {host name} {ports name} {size} {copy of .csv file name}`

Note:
- If you're running more than one brokers, write the ports name like the following:
`port1,port2,port3`