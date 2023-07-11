#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql
from mysql.connector import Error
from datetime import datetime

# MySQL connection configuration
config = {
    'host': '127.0.0.1',
    'port':'3306',
    'database': 'kafka',
    'user': 'root',
    'password': 'root'
}


# In[2]:


num_records = 4

while True:
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(**config)

        if connection.is_connected():
            print("Connected to MySQL")

            # Create a cursor
            cursor = connection.cursor()

            # Generate dummy data or prepare dynamic records
            records = []
            for i in range(num_records):
                # Example: Generate a name with a timestamp
                name = f"Record_{i+1}_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

                # Append the record to the list
                records.append((name,))

            # Insert records into the table
            query = "INSERT INTO test (name) VALUES (%s)"
            cursor.executemany(query, records)
            connection.commit()

            print(f"Inserted {num_records} records")

    except Error as e:
        print("Error connecting to MySQL:", e)

    finally:
        # Close the database connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed")

    # Delay between iterations
    # Adjust the duration according to your needs
    # For example, sleep for 1 minute (60 seconds)
    import time
    time.sleep(60)


# In[ ]:




