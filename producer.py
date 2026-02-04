import asyncio
import json
from datetime import UTC, datetime

from aiokafka import AIOKafkaProducer


async def create_producer() -> AIOKafkaProducer:
    """Create and start an async Kafka producer."""
    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:29092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer.start()
    return producer


async def send_messages(
    producer: AIOKafkaProducer, topic: str, count: int = 10
) -> None:
    """Send sample messages to a Kafka topic."""
    for i in range(count):
        message = {
            "id": i,
            "message": f"Hello from async producer! Message #{i}",
            "timestamp": datetime.now(UTC).isoformat(),
        }
        key = f"key-{i % 3}"  # Rotate between 3 keys for partition distribution

        result = await producer.send_and_wait(topic, value=message, key=key)
        print(
            f"Sent: {message['message']} -> "
            f"partition={result.partition}, offset={result.offset}"
        )
        await asyncio.sleep(1)  # Simulate some work between messages


async def main() -> None:
    topic = "demo-topic"
    producer = await create_producer()

    try:
        print(f"Starting producer, sending messages to '{topic}'...")
        await send_messages(producer, topic, count=10)
        print("All messages sent!")
    finally:
        await producer.stop()
        print("Producer stopped.")


if __name__ == "__main__":
    asyncio.run(main())
