#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from confluent_kafka import Consumer, KafkaException

kafka_config = {
    'bootstrap.servers': 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092',  # Kafka bootstrap servers
    'group.id': 'groupid1',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
}

kafka_topic = 'test'

# Create a Kafka consumer with the necessary configurations
consumer = Consumer(kafka_config)

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
            print(f"Received message: {message_value}")

except KeyboardInterrupt:
    pass

finally:
    # Close the consumer to release resources
    consumer.close()


# In[ ]:




