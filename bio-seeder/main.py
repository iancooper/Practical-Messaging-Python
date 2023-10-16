from confluent_kafka import Producer
import json

# Sample data
biographies_data = [
    {"name": "Clarissa Harlow", "biography": "A young woman whose quest for virtue is continually thwarted by her family."},
    {"name": "Pamela Andrews", "biography": "A young woman whose virtue is rewarded."},
    {"name": "Harriet Byron", "biography": "An orphan, and heir to a considerable fortune of fifteen thousand pounds."},
    {"name": "Charles Grandison", "biography": "A man of feeling who truly cannot be said to feel."},
]

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka bootstrap servers
}

# Kafka topic
topic = 'biography'


# Callback function to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


# Create Kafka producer
producer = Producer(conf)

# Produce messages to Kafka topic
for biography in biographies_data:
    # Convert biography data to JSON
    message_value = json.dumps(biography)

    # Produce message to Kafka
    producer.produce(topic, value=message_value, callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()
