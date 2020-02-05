#!/user/bin/python3.8

from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from datetime import datetime, date
from time import sleep, monotonic_ns
import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument("-b", "--kafka_broker", help="Set ip/hostname to kafka broker, host:port", type=str, required=True)
parser.add_argument("-n", "--number_of_messages", help="Set ip/hostname to kafka broker, host:port", type=int, default=100)
parser.add_argument("-t", "--topic", help="Topic to do tests on(NOTE: Topic will be deleted)", type=str,  default="_latency_test")
parser.add_argument("-c", "--consumer_timeout_ms", help="Consumer timeout in ms", type=int, default="10")
args = parser.parse_args()

kafka_brokers = args.kafka_broker
number_of_msgs = args.number_of_messages
topic = args.topic
consumer_timeout_ms = args.consumer_timeout_ms

msg_send = []
msg_recv = []

admin = KafkaAdminClient(bootstrap_servers=kafka_brokers)

try:
    admin.delete_topics([topic], timeout_ms=5000)
except:
    print("Cannot delete topic, might not exist")


consumer = KafkaConsumer(bootstrap_servers=kafka_brokers,auto_offset_reset='earliest',consumer_timeout_ms=consumer_timeout_ms)
producer = KafkaProducer(bootstrap_servers=kafka_brokers)

consumer.subscribe(topic)



for i in range(number_of_msgs):
	now_start = round(monotonic_ns())
	producer.send(topic, bytes(json.dumps({
	       "@timestamp": now_start, 
	       "sender":"latency_tester",
	       "payload":{
	           "action":"add_neuro_program",
	           "neuro_program":"TestNeuron",
	           }
	       } ),'utf-8'))
	producer.flush()

	for message in consumer:
	    now_end = round(monotonic_ns())
	    msg_recv.append(now_end)

	msg_send.append(now_start)

assert(len(msg_send) == len(msg_recv))

time_diff = []
for i in range(number_of_msgs):
	send = msg_send[i]
	recv = msg_recv[i]
	time_diff.append(recv-send)


diff_sum = 0
for time in time_diff:
	diff_sum += time

avg_diff = diff_sum/number_of_msgs

print("avg time diff: {}". format(float(avg_diff/10e6)))

time_diff.sort()
print("Median: {}".format(float(time_diff[len(time_diff)//2])/10e6))
