import datetime
import io
from time import sleep

from confluent_kafka import Consumer, KafkaError
from minio import Minio

# Constants
BROKER = 'localhost:9094'
TOPIC = 'cpu_data'
CONSUMER_CONFIG = {
    'bootstrap.servers': BROKER,
    'group.id': 'cpu_group',
    'auto.offset.reset': 'earliest'
}
MINIO_ENDPOINT='localhost:9000'
MINIO_ROOT_USER='alcantara'
MINIO_ROOT_PASSWORD='senhasegura'
MINIO_DEFAULT_BUCKETS='baldinho'


def consume_messages():
    """Consume messages from Kafka and write them to a JSON file."""
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe([TOPIC])

    try:
        while True:
            message = consumer.poll(1.0)
            
            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaError(message.error())
            
            print(f"Received message: key={message.key()}")
    
            minio_put_message(message.value(), message.key().decode())
            # sleep(1)
            
    except KeyboardInterrupt:
        print("\nConsumer interrupted by user.")
    finally:
        consumer.close()
        print("Consumer closed gracefully.")


def create_minio_client() -> Minio:
    """ Create a Minio client and return a minio object """
    minio_cli = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False
        )
    return minio_cli


def minio_put_message(message, name) -> None:
    """
    Upload a message to Minio.

    Parameters:
    message (str): The message to be uploaded.
    """
    minio_cli.put_object(
        bucket_name=MINIO_DEFAULT_BUCKETS,
        object_name=f'cpu_data_{name}',
        data=io.BytesIO(message),
        length=len(message),
        content_type='application/json'
    )


if __name__ == "__main__":
    minio_cli = create_minio_client()
    consume_messages()
