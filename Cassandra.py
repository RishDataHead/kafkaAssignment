#!/usr/bin/env python
# coding: utf-8

# In[1]:


from confluent_kafka import Consumer, KafkaException
from cassandra.cluster import Cluster

kafka_config = {
    'bootstrap.servers': 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092',  # Kafka bootstrap servers
    'group.id': 'groupid1',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
}

kafka_topic = 'test'

# Cassandra configuration
cassandra_config = {
    'contact_points': ['your_contact_point'],  # Cassandra contact points
    'keyspace': 'your_keyspace'  # Keyspace name
}

# Create a Kafka consumer with the necessary configurations
consumer = Consumer(kafka_config)

# Create a Cassandra cluster connection
cluster = Cluster(contact_points=cassandra_config['contact_points'])
session = cluster.connect(cassandra_config['keyspace'])

# Subscribe to the Kafka topic
consumer.subscribe([kafka_topic])

# Continuously poll for new messages
try:
    while True:
        # Poll for messages from Kafka
        message = consumer.poll(timeout=1.0)

        if message is None:
            continue
        elif message.error():
            raise KafkaException(message.error())
        else:
            # Process the received message
            message_value = message.value()
            transformed_data = transform_message(message_value)  # Custom transformation function
            
            # Write the transformed data to the Cassandra table
            insert_data_to_cassandra(session, transformed_data)  # Custom Cassandra insertion function

except KeyboardInterrupt:
    pass

finally:
    # Close the consumer and Cassandra session to release resources
    consumer.close()
    session.shutdown()
    cluster.shutdown()


# In[ ]:




