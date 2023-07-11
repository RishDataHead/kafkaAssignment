#!/usr/bin/env python
# coding: utf-8

# In[1]:


import argparse
from uuid import uuid4
#from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List


# In[ ]:


import mysql.connector
from mysql.connector import Error
from confluent_kafka import KafkaException, Producer
import json

# MySQL connection configuration
mysql_config = {
    'host': '127.0.0.1',
    'port':'3306',
    'database': 'kafka',
    'user': 'root',
    'password': 'root'
}

# Kafka and Schema Registry configuration
kafka_config = {
    'bootstrap.servers': 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092',  # Kafka bootstrap servers
}



# Kafka topic to publish the data
kafka_topic = 'test'

# Connect to MySQL
try:
    connection = mysql.connector.connect(**mysql_config)

    if connection.is_connected():
        print("Connected to MySQL")

        # Create a cursor
        cursor = connection.cursor()

        # Create a Kafka producer with the necessary configurations
        producer = Producer(kafka_config)

        # Set the initial timestamp for incremental reading
        last_timestamp = None

        while True:
            # Retrieve rows incrementally based on created_at column
            if last_timestamp is None:
                # Initial retrieval, get the earliest timestamp
                cursor.execute("SELECT * FROM test ORDER BY created_at ASC LIMIT 1")
            else:
                # Incremental retrieval, get rows after the last timestamp
                cursor.execute("SELECT * FROM test WHERE created_at > %s", (last_timestamp,))

            rows = cursor.fetchall()

            if rows:
                for row in rows:
                    # Process each row as needed
                    # Example: Convert row to JSON payload

                    # Construct the JSON message payload
                    message_payload = {
                        'id': row[0],
                        'name': row[1],
                        'created_at': row[2].strftime('%Y-%m-%d %H:%M:%S')
                    }

                    # Convert the JSON payload to a string
                    json_payload = json.dumps(message_payload)

                    # Produce the JSON message to the Kafka topic
                    producer.produce(topic=kafka_topic, value=json_payload)

                # Flush the producer to ensure messages are sent
                producer.flush()

                # Update the last timestamp
                last_timestamp = rows[-1][2].strftime('%Y-%m-%d %H:%M:%S')

            else:
                # No more rows to fetch, wait or break the loop
                # Example: wait for new data to be inserted
                import time
                time.sleep(5)

except Error as e:
    print("Error connecting to MySQL:", e)

except KafkaException as ke:
    print("Error producing to Kafka:", ke)

finally:
    # Close the database connection
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection closed")


# In[ ]:




