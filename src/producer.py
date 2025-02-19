import datetime
import subprocess
import json
from time import sleep

from confluent_kafka import Producer

# Constants
BROKER = 'localhost:9094'
TOPIC = 'cpu_data'
CMD = ['mpstat', '-o', 'JSON']  # Bash command for data generation
SLEEP_INTERVAL = 1  # Time interval between data collection (in seconds)


def get_data(cmd: list) -> str:
    """
    Run a command and return the output.

    Args:
        cmd (list): The command to execute as a list of arguments.

    Returns:
        str: The stdout of the command as a string.
    """
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, check=True)
        return result.stdout.decode('utf-8')
    except subprocess.CalledProcessError as e:
        print(f"Command failed with error: {e}")
        return ""
    except Exception as e:
        print(f"Unexpected error: {e}")
        return ""


def delivery_report(err, msg):
    """
    Callback function to handle delivery reports for Kafka messages.

    Args:
        err: Error information (if any).
        msg: The Kafka message object.
    """
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} key=[cpu_data_{msg.key().decode()}]")


def produce_messages():
    """
    Continuously collect CPU data and produce messages to Kafka.
    """
    # Producer configuration
    producer_config = {
        'bootstrap.servers': BROKER,
        'security.protocol': 'PLAINTEXT'
    }

    # Create Kafka producer
    producer = Producer(producer_config)

    while True:
        # Collect CPU data
        data = get_data(CMD)
        if not data:
            print("No data received. Skipping this iteration.")
            sleep(SLEEP_INTERVAL)
            continue

        # Encode data to JSON and produce Kafka message

        key = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S').encode('utf-8')
        try:
            value = json.dumps(data, ensure_ascii=False).encode('utf-8')
            producer.produce(TOPIC, key=key, value=value, callback=delivery_report)
            producer.flush()  # Ensure the message is sent
        except Exception as e:
            print(f"Error producing message: {e}")

        # Wait before the next iteration
        sleep(SLEEP_INTERVAL)


if __name__ == '__main__':
    produce_messages()
