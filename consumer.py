import asyncio
import json
import signal
from contextlib import suppress

from aiokafka import AIOKafkaConsumer


async def create_consumer(topic: str, group_id: str) -> AIOKafkaConsumer:
    """Create and start an async Kafka consumer."""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers="localhost:29092",
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",  # Start from beginning if no committed offset
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
    )
    await consumer.start()
    return consumer


async def consume_messages(
    consumer: AIOKafkaConsumer, stop_event: asyncio.Event
) -> None:
    """Consume messages from Kafka until stop event is set."""
    try:
        async for msg in consumer:
            print(
                f"Received: topic={msg.topic}, partition={msg.partition}, "
                f"offset={msg.offset}, key={msg.key}"
            )
            print(f"  Value: {msg.value}")
            print()

            if stop_event.is_set():
                break
    except asyncio.CancelledError:
        pass


async def main() -> None:
    topic = "demo-topic"
    group_id = "demo-consumer-group"
    stop_event = asyncio.Event()

    # Handle graceful shutdown
    loop = asyncio.get_running_loop()

    def signal_handler() -> None:
        print("\nShutdown signal received...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, signal_handler)

    consumer = await create_consumer(topic, group_id)

    try:
        print(f"Starting consumer, listening on '{topic}'...")
        print("Press Ctrl+C to stop.\n")
        await consume_messages(consumer, stop_event)
    finally:
        await consumer.stop()
        print("Consumer stopped.")


if __name__ == "__main__":
    asyncio.run(main())
