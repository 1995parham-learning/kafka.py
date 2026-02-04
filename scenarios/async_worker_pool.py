"""
Scenario: Async Worker Pool Pattern

The proper way to handle slow processing in aiokafka:
- Use a worker pool to process messages concurrently
- Consumer stays responsive (heartbeats keep flowing)
- Control concurrency with a semaphore
- Track in-flight messages for proper offset commits

Run this:
    uv run python scenarios/async_worker_pool.py
"""

import asyncio
import json
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

TOPIC = "async-worker-demo"
BOOTSTRAP_SERVERS = "localhost:29092"
GROUP_ID = "async-worker-group"


@dataclass
class InFlightMessage:
    """Track a message being processed."""

    msg_id: int
    partition: int
    offset: int
    task: asyncio.Task


@dataclass
class AsyncWorkerConsumer:
    """
    Consumer with async worker pool for concurrent message processing.

    Key features:
    - Semaphore limits concurrent processing
    - Messages processed in parallel without blocking consumer
    - Commits only after processing completes
    - Graceful shutdown waits for in-flight messages
    """

    topic: str
    group_id: str
    max_concurrent: int = 5

    consumer: AIOKafkaConsumer | None = field(default=None, init=False)
    in_flight: dict[int, InFlightMessage] = field(default_factory=dict, init=False)
    semaphore: asyncio.Semaphore = field(init=False)
    processed_count: int = field(default=0, init=False)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event, init=False)

    def __post_init__(self):
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

    async def start(self):
        """Start the consumer."""
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=self.group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            # Reasonable timeouts - we won't hit them with async processing
            max_poll_interval_ms=300000,
            session_timeout_ms=45000,
        )
        await self.consumer.start()
        print(f"  Consumer started (max_concurrent={self.max_concurrent})")

    async def stop(self):
        """Graceful shutdown - wait for in-flight messages."""
        self.stop_event.set()

        if self.in_flight:
            print(f"\n  Waiting for {len(self.in_flight)} in-flight messages...")
            tasks = [m.task for m in self.in_flight.values()]
            await asyncio.gather(*tasks, return_exceptions=True)

        if self.consumer:
            await self.consumer.stop()
        print(f"  Consumer stopped. Total processed: {self.processed_count}")

    async def process_message(self, msg_id: int, data: dict) -> bool:
        """
        Process a single message. Override this for your business logic.
        Returns True if successful, False if failed.
        """
        processing_time = data.get("processing_time", 1)

        # Simulate async I/O work (API calls, DB queries, etc.)
        await asyncio.sleep(processing_time)

        # Simulate random failures (10% chance)
        if random.random() < 0.1:
            raise ValueError(f"Random processing error for message {msg_id}")

        return True

    async def _worker(self, msg):
        """Worker that processes a message with semaphore control."""
        assert self.consumer is not None
        msg_id = msg.value["id"]
        tp = TopicPartition(msg.topic, msg.partition)

        async with self.semaphore:  # Limit concurrency
            try:
                print(f"  [{msg_id}] Processing started...")
                await self.process_message(msg_id, msg.value)
                print(f"  [{msg_id}] ✓ Done")

                # Commit this message's offset
                await self.consumer.commit({tp: msg.offset + 1})
                self.processed_count += 1

            except Exception as e:
                print(f"  [{msg_id}] ✗ Failed: {e}")
                # In production: send to DLQ here
                # For now, commit anyway to avoid reprocessing loop
                await self.consumer.commit({tp: msg.offset + 1})

            finally:
                # Remove from in-flight tracking
                if msg_id in self.in_flight:
                    del self.in_flight[msg_id]

    async def run(self, max_messages: int | None = None):
        """
        Main consumer loop.

        Messages are immediately dispatched to workers.
        Consumer loop stays fast, heartbeats keep flowing.
        """
        assert self.consumer is not None
        count = 0

        async for msg in self.consumer:
            if self.stop_event.is_set():
                break

            msg_id = msg.value["id"]

            # Spawn worker task (non-blocking)
            task = asyncio.create_task(self._worker(msg))
            self.in_flight[msg_id] = InFlightMessage(
                msg_id=msg_id,
                partition=msg.partition,
                offset=msg.offset,
                task=task,
            )

            in_flight_count = len(self.in_flight)
            print(f"  [{msg_id}] Received, dispatched (in-flight: {in_flight_count})")

            count += 1
            if max_messages and count >= max_messages:
                break

        # Wait for remaining workers
        if self.in_flight:
            print(f"\n  Draining {len(self.in_flight)} remaining messages...")
            await asyncio.gather(
                *[m.task for m in self.in_flight.values()], return_exceptions=True
            )


async def produce_messages(count: int = 15):
    """Send test messages with varying processing times."""
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()

    try:
        print(f"\n{'=' * 60}")
        print("PRODUCING MESSAGES")
        print("=" * 60)

        for i in range(count):
            # Random processing time between 1-5 seconds
            processing_time = random.randint(1, 5)
            message = {
                "id": i,
                "data": f"Message {i}",
                "processing_time": processing_time,
                "timestamp": datetime.now(UTC).isoformat(),
            }
            await producer.send_and_wait(TOPIC, value=message)
            print(f"  Message {i}: processing_time={processing_time}s")

    finally:
        await producer.stop()


async def reset_consumer_group():
    """Reset consumer group for clean demo."""
    try:
        consumer = AIOKafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()
        await consumer.seek_to_beginning()
        for tp in consumer.assignment():
            await consumer.commit({tp: 0})
        await consumer.stop()
    except Exception:
        pass


async def main():
    print("\n" + "=" * 60)
    print("ASYNC WORKER POOL PATTERN")
    print("=" * 60)
    print("""
This pattern solves slow processing by:

1. Dispatching messages to async workers immediately
2. Using a semaphore to limit concurrent processing
3. Consumer loop stays fast -> heartbeats keep flowing
4. Workers process in parallel -> high throughput

Key insight:
  aiokafka sends heartbeats in a background asyncio task.
  As long as you don't BLOCK the event loop, heartbeats work.

  await asyncio.sleep(10)  # OK - event loop is free
  time.sleep(10)           # BAD - blocks everything!
""")

    await reset_consumer_group()
    await produce_messages(15)

    print(f"\n{'=' * 60}")
    print("CONSUMING WITH ASYNC WORKER POOL")
    print("=" * 60)
    print("max_concurrent=5: Up to 5 messages processed in parallel\n")

    worker = AsyncWorkerConsumer(
        topic=TOPIC,
        group_id=GROUP_ID,
        max_concurrent=5,
    )

    await worker.start()

    try:
        await asyncio.wait_for(worker.run(max_messages=15), timeout=60.0)
    except TimeoutError:
        pass
    finally:
        await worker.stop()

    print(f"\n{'=' * 60}")
    print("KEY POINTS")
    print("=" * 60)
    print("""
1. Messages dispatched instantly to workers (non-blocking)
2. Semaphore controls max concurrent processing
3. Each worker commits its own offset when done
4. Graceful shutdown waits for in-flight messages
5. Consumer never blocks -> no timeout issues!

Caveats:
- Out-of-order commits possible (message 5 may commit before 3)
- Use DLQ for failed messages (shown in dead_letter_queue.py)
- For strict ordering, process sequentially per partition
""")


if __name__ == "__main__":
    asyncio.run(main())
