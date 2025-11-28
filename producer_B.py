"""
Task A — Kafka Producer (OPTION 2 — Updated)

Create a Kafka producer that:
    • Connects to localhost:9092
    • Sends JSON messages every 1.5 seconds (configurable)
    • Each message must conform to the updated schema (see below)
    • Sends messages to topic: (your choice)

Message schema:
{
    "event_id": "<uuid>",
    "event_type": "<created|updated|deleted>",
    "created_at": <unix_ms>,
    "payload": {
        "user_id": <int>,
        "score": <float>,
        "tags": ["tag1", "tag2", ...]
    }
}

Requirements:
    • Use KafkaProducer with value_serializer as JSON
    • Print each message as a compact log line before or after sending
    • Stop gracefully on KeyboardInterrupt and flush pending messages
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import json
import time
import uuid
import random
import string
import os

# Configuration/constants (students may change these)
TOPIC_NAME =  # TODO: name your topic
MIN_TAGS = 1                          
MAX_TAGS = 3
SEND_INTERVAL = 1.5  # seconds

def random_tag(length=5):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))

def generate_tags():
    count = random.randint(MIN_TAGS, MAX_TAGS)
    return [random_tag() for _ in range(count)]

def create_producer():
    """
    Create and return a KafkaProducer instance configured with JSON serializer.
    """
    try:
        # TODO: Create Kafka producer using JSON serialization and localhost:9092
        
        
        return producer
    except NoBrokersAvailable as e:
        # Handle the exception 


# Keep main loop in a function to allow import/mocking in tests if needed
def run_producer(topic=TOPIC_NAME, interval=SEND_INTERVAL):
    producer = create_producer()

    print(f"Producer started. Sending to topic '{topic}'...\n")

    try:
        while True:
            # TODO: Create message using schema
            
            message = {
                "event_id": , # TODO: generate UUID from uuid module
                "event_type": , # TODO: choose random from ["created", "updated", "deleted"]
                "created_at": int(time.time() * 1000),
                "payload": {
                    "user_id": ,  #TODO generate random int between 1 and 1000
                    "score": round(random.uniform(0, 100), 2),
                    "tags": , # TODO generate list of random tags
                },
            }

            # TODO: Send this message to Kafka (consider catching KafkaError)
            try:
                
            except KafkaError as e:
                print() # Handle send error
    

            # TODO: Print event_id, event_type, user_id
            print(f"[SENT] {}  type={}  user={}")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        # Flush pending messages then close
        producer.flush()
        producer.close()
       

if __name__ == "__main__":
    run_producer()