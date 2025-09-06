import argparse
import datetime
import json
import random
import time
import uuid
from enum import StrEnum
from string import ascii_uppercase

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer


class FillingEnum(StrEnum):
    BOTH = "both"
    ACTIVITY = "activity"
    TRANSACTIONS = "transactions"


class RemovingEnum(StrEnum):
    BOTH = "both"
    ACTIVITY = "activity"
    TRANSACTIONS = "transactions"
    NO = "no"


parser = argparse.ArgumentParser()
parser.add_argument("--fill", type=str, default=FillingEnum.BOTH, choices=FillingEnum.__members__)
parser.add_argument("--remove", type=str, default=RemovingEnum.NO, choices=RemovingEnum.__members__)

args = parser.parse_args()
# Configuration
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081"
TRANSACTION_TOPIC_NAME = "transactions"
ACTIVITY_TOPIC_NAME = "user-activity"


transactions_schema = """
{
  "type": "record",
  "name": "Transaction",
  "fields": [
    {
        "name": "transaction_id",
        "type": "string",
        "logicalType": "uuid"
    },
    {
        "name": "user_id",
        "type": "string",
        "logicalType": "uuid"
    },
    {
        "name": "amount",
        "type": "float"
    },
    {
        "name": "merchant",
        "type": "string"
    },
    {
        "name": "currency",
        "type": "string"
    },
    {
        "name": "timestamp",
        "type": {
            "type": "long",
            "logicalType": "timestamp-millis"
        }
    },
    {
        "name": "is_fraud",
        "type": "boolean"
    }
  ]
}
"""

user_activity_schema = """
{
  "type": "record",
  "name": "UserActivity",
  "fields": [
    {
        "name": "event_id",
        "type": "string",
        "logicalType": "uuid"
    },
    {
        "name": "user_id",
        "type": "string",
        "logicalType": "uuid"
    },
    {
        "name": "event_type",
        "type": {
            "type": "enum",
            "name": "Event",
            "symbols": ["click", "view", "add_to_cart", "purchase"]
        }
    },
    {
        "name": "device",
        "type": {
            "type": "enum",
            "name": "Browser",
            "symbols": ["mobile", "desktop", "tablet"]
        }
    },
    {
        "name": "browser",
        "type": {
            "type": "enum",
            "name": "Device",
            "symbols": ["Chrome", "Safari", "Firefox", "Edge"]
        }
    },
    {
        "name": "timestamp",
        "type": {
            "type": "long",
            "logicalType": "timestamp-millis"
        }
    }
  ]
}
"""

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

# transaction_serializer = AvroSerializer(schema_registry_client, transactions_schema)
# activity_serializer = AvroSerializer(schema_registry_client, user_activity_schema)


producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "key.serializer": StringSerializer("utf_8"),  # Corrected line
    "value.serializer": StringSerializer("utf_8"),
}

producer = SerializingProducer(producer_conf)
if args.remove in (RemovingEnum.TRANSACTIONS, RemovingEnum.BOTH):
    schema_registry_client.delete_subject("transactions-value")  # Clear old schema to prevent exception
if args.remove in (RemovingEnum.TRANSACTIONS, RemovingEnum.BOTH):
    schema_registry_client.delete_subject("user-activity-value")  # Clear old schema to prevent exception
producer.flush()

# Changed avro to json because Spark from_avro raise 500 on my PC (Mac silicon)

for _ in range(10000):
    time.sleep(5)

    user_id = str(uuid.uuid4())
    if args.fill.lower() in (FillingEnum.TRANSACTIONS, FillingEnum.BOTH):
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "user_id": user_id,
            "amount": round(random.uniform(1000.0, 50000.99), 2),
            "merchant": random.choice(("Stripe", "Paypal", "Banking", "LiqPay", "NovaPay")),
            "currency": "".join(random.choice(ascii_uppercase) for _ in range(3)),
            "timestamp": (datetime.datetime.now() + datetime.timedelta(minutes=random.randint(-5, 5))).isoformat(),
            "is_fraud": bool(random.randint(0, 1)),
        }
        # producer._value_serializer = transaction_serializer
        producer.produce(topic=TRANSACTION_TOPIC_NAME, key=str(user_id), value=json.dumps(transaction))
        print(transaction)
        print("=" * 10)
    if args.fill.lower() in (FillingEnum.ACTIVITY, FillingEnum.BOTH):
        activity = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "event_type": random.choice(("click", "view", "add_to_cart", "purchase")),
            "device": random.choice(("mobile", "desktop", "tablet")),
            "browser": random.choice(("Chrome", "Safari", "Firefox", "Edge")),
            "timestamp": (datetime.datetime.now() + datetime.timedelta(minutes=random.randint(-5, 5))).isoformat(),
        }
        # producer._value_serializer = activity_serializer
        producer.produce(topic=ACTIVITY_TOPIC_NAME, key=user_id, value=json.dumps(activity))
        print(activity)
        print("=" * 10)


producer.flush()
print("All messages sent.")
