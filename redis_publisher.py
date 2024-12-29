import random
from datetime import datetime, timedelta
import time
import uuid
import redis

# Redis connection details (modify host and port if needed)
redis_host = "localhost"
redis_port = 6379

target_duration = timedelta(minutes=1)
batch_size = 1000

def publisher():
    try:
        connection = redis.Redis(host=redis_host, port=redis_port)
    except redis.ConnectionError:
        print("Error: Failed to connect to Redis server")
        exit(1)

    start_time = datetime.now()
    total_messages = 0

    try:
        while datetime.now() - start_time < target_duration:
            p = connection.pipeline()
            for _ in range(batch_size):
                p.publish(
                    "messages:published", 
                    f'{{"message_id": "{str(uuid.uuid4())}"}}'
                )
            p.execute()
            total_messages += batch_size
            time.sleep(random.uniform(0.1, 0.5))
    except Exception as e:
        print(f"Error: {e}")
    finally:
        print(f"Total messages published: {total_messages}")

if __name__ == "__main__":
    publisher()
