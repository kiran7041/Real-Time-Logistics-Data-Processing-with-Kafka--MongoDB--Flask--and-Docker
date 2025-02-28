#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import os
import json
from pymongo import MongoClient



# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': '',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '',
    'sasl.password': '',
    'group.id': 'group2',
    'auto.offset.reset': 'earliest'
}



# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': '',
    'basic.auth.user.info': '{}:{}'.format('', '')
})

# Fetch the latest Avro schema for the value
subject_name = 'logistics_data_2-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)



# MongoDB configuration
mongo_client = MongoClient('')  # Replace with your MongoDB connection string
db = mongo_client['supply_chain_db']  # Replace with your database name
collection = db['logistics_data_2']  # Replace with your collection name



# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
})

# Subscribe to the 'logistic_data' topic
consumer.subscribe(['logistics_data_2'])




# Process and insert Avro messages into MongoDB
try:
    while True:
        msg = consumer.poll(1.0)  # Adjust the timeout as needed

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue

        # Deserialize Avro data
        value = msg.value()
        print("Received message:", value)
        
        # Data validation checks
        if 'BookingID' not in value or value['BookingID'] is None:
            print("Skipping message due to missing or null 'BookingID'.")
            continue

        # Data type validation checks
        if not isinstance(value['BookingID'], str):
            print("Skipping message due to 'BookingID' not being a string.")
            continue
        
        #We can add more checks as needed
        
        # Check if a document with the same 'BookingID' exists
        existing_document = collection.find_one({'BookingID': value['BookingID']})

        if existing_document:
            print(f"Document with BookingID '{value['BookingID']}' already exists. Skipping insertion.")
        else:
            # Insert data into MongoDB
            collection.insert_one(value)
            print("Inserted message into MongoDB:", value)


except KeyboardInterrupt:
    pass
finally:
    # Commit the offset to mark the message as processed
    consumer.commit()
    consumer.close()
    mongo_client.close()




