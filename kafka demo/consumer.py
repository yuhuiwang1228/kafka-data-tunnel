import json
import random
import string
import sys

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer

from employee import Employee
from producer import employee_topic_name


class CaphcaConsumer:
    string_deserializer = StringDeserializer('utf_8')

    def __init__(self, host: str = "localhost", port: str = "29092",
                 group_id: str = ''.join(random.choice(string.ascii_lowercase) for i in range(10))):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        self.consumer = Consumer(self.conf)
        self.keep_runnning = True

    def consume(self, topics, processing_func):
        try:
            self.consumer.subscribe(topics)
            while self.keep_runnning:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    print("Couldn't find a single message...")
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError.PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    processing_func(msg)
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()


def persist_employee(msg):
    e = Employee(**(json.loads(msg.value())))
    employee = (e.first_name, e.last_name, e.dob, e.city)
    print(employee)
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5434,
            database="postgres",
            user="postgres",
            password="postgres")
        conn.autocommit = True

        # create a cursor
        cur = conn.cursor()
        if e.action=='INSERT':
            result = cur.execute(
                "INSERT INTO employees_b (first_name,last_name,dob,city) VALUES (%s,%s,%s,%s) on conflict do nothing", employee)
        elif e.action=='UPDATE':
            result = cur.execute(
                "UPDATE employees_b SET first_name = %s, last_name = %s, dob = %s, city = %s WHERE emp_id = %s", employee)
        elif e.action=='DELETE':
            result = cur.execute(
                "DELETE FROM employees_b WHERE emp_id = %s", (e.emp_id,))
        print(result)
        cur.close()
    except Exception as e:
        print(e)


if __name__ == '__main__':
    consumer = CaphcaConsumer(group_id="employee_consumer_1")
    consumer.consume([employee_topic_name], persist_employee)