"""
A quick Python Kafka Consumer to validate data published to Kafka
by the DECS Java job.
"""
from collections import Counter
from confluent_kafka import Consumer, KafkaException
import json
import logging
import pprint

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("_testPythonConsumer_")

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "testPythonConsumers",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
)

if __name__ == "__main__":
    consumer.subscribe(["txa-decs-ingest"])
    try:
        while True:
            messages = consumer.consume(
                            num_messages=1, timeout=5
                        )
            if not messages:  # empty result set, nothing to process
                logger.info("No messages found")
                continue
            logger.info(f"Polled {len(messages)} messages")
            partitions = [m.partition() for m in messages]
            partition_counter = Counter(partitions)
            for partition, count in partition_counter.items():
                logger.info(f"Got {count} messages from partition {partition}")

            logger.info("Deserializing messages...")
            for message in messages:
                if message.error():
                    logger.error(f"Error consuming a message: {message.error()}")
                    raise KafkaException(message.error())
                else:
                    # Deserialize Messages
                    key = message.key().decode('utf-8')
                    value = json.loads(message.value())
                    logger.info(f"Got message with KEY: {key}")
                    logger.info(f"And VALUE {pprint.pformat(value)}")

    except (Exception, KeyboardInterrupt):
        logger.info("Some exception occurred...")
        logger.info("Closing consumer then raising exception...")
        consumer.close()
        raise
