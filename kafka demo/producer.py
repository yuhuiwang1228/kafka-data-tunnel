import csv
import json
import psycopg2

from confluent_kafka.serialization import StringSerializer
from kafka import KafkaProducer
from employee import Employee

employee_topic_name = "employee_data_8thDec"


class CaphcaProducer:
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        self.producer = KafkaProducer(bootstrap_servers=('%s:%s' % (self.host, self.port)))

    def producer_msg(self, topic_name, key, value):
        self.producer.send(topic=topic_name, key=key, value=value).get(timeout=5)


class DatabaseFetcher:
    def __init__(self, host="localhost", port=5432, dbname="postgres", user="postgres", password="postgres"):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )

    def fetch_cdc_data(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT emp_id, first_name, last_name, dob, city, action FROM employees_cdc")
            rows = cur.fetchall()
            return rows

if __name__ == '__main__':
    db_fetcher = DatabaseFetcher() # ("localhost", 5432, "postgres", "postgres", "postgres")
    cdc_data = db_fetcher.fetch_cdc_data()

    producer = CaphcaProducer()
    for row in cdc_data:
        # emp = Employee(emp_id=row[0], first_name=row[1], last_name=row[2], dob=row[3], city=row[4], action=row[5])  # Assuming Employee can be instantiated like this
        emp = Employee.from_csv_line(row)
        print(emp.to_json())
        producer.producer_msg(employee_topic_name, key=f"{emp.emp_id}".encode(), value=emp.to_json().encode())


# class CsvReader:
#     def read_csv(self, csv_file='../../resources/employees.csv'):
#         with open(csv_file, newline='') as csvfile:
#             rows = []
#             reader = csv.reader(csvfile, delimiter=',', quotechar='|')
#             for row in reader:
#                 # print(', '.join(row))
#                 rows.append(row)
#             return rows


# if __name__ == '__main__':
#     reader = CsvReader()
#     producer = CaphcaProducer()
#     lines = reader.read_csv('./employees.csv')
#     for line in lines[1:]:
#         emp = Employee.from_csv_line(line)
#         producer.producer_msg(employee_topic_name, key=f"{emp.emp_id}".encode(), value=emp.to_json().encode())