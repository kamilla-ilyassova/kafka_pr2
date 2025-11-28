"""
Task B — Kafka Consumer

Changes from previous version:
    • Reads updated, nested message schema
    • Prints messages in multi-line detailed format
    • Counts total messages
    • Reads from earliest offset
"""

import json

TOPIC_NAME = "my_topic"       # TODO: ensure it matches producer
GROUP_ID = "lab-extended-consumer"  


# TODO: Create consumer with earliest offset reset
consumer = 

print(f"Consumer  started. Listening to '{TOPIC_NAME}'...\n")

count = 0

try:
    # for each message in the consumer loop and increment count
    for msg in consumer:
        #TODO: extract message value
        
        # TODO: Print detailed multi-line message
        print(
            f"\n=== RECEIVED MESSAGE #{count} ===\n"
            f"event_id:   {}\n"
            f"event_type: {}\n"
            f"created_at: {}\n"
            f"payload:\n"
            f"    user_id: {}\n"
            f"    tags:    {}\n"
            f"metadata:\n"
            f"    partition: {}\n"
            f"    offset:    {}\n"
            f"===============================\n"
        )

except KeyboardInterrupt:
    print(f"\nStopping consumer... Total messages read: {count}")
    consumer.close()