# Adapted from producer_tax_json.py

import csv
from json import dumps
from kafka import KafkaProducer
from time import sleep


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# Different file
file = open('../avro_example/data/green_tripdata_2020-07.csv')

# Different topic
csvreader = csv.reader(file)
header = next(csvreader)
for row in csvreader:
    key = {"vendorId": int(row[0])}
    value = {"vendorId": int(row[0]), "trip_distance": float(row[4]), "total_amount": float(row[16])}
    producer.send('datatalkclub.green_taxi_ride.json', value=value, key=key)
    print("producing")
    sleep(1)