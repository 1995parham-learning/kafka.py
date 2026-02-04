"""
Scenario: In-Flight Message Tracking

In-flight messages are messages that have been:
  ✓ Received from Kafka
  ✗ Not yet processed
  ✗ Not yet committed

This demo shows exactly what each in-flight message is doing at any moment.

Run this:
    uv run python scenarios/inflight_tracking.py
"""

import asyncio
import contextlib
import json
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

TOPIC = "inflight-demo"
BOOTSTRAP_SERVERS = "localhost:29092"
GROUP_ID = "inflight-demo-group"


class MessageState(Enum):
    QUEUED = "queued"  # Received, waiting for worker
    PROCESSING = "processing"  # Worker is processing
    COMMITTING = "committing"  # Processing done, committing offset
    DONE = "done"  # Fully complete


@dataclass
class InFlightMessage:
    """Detailed tracking of an in-flight message."""

    msg_id: int
    partition: int
    offset: int
    received_at: datetime
    state: MessageState = MessageState.QUEUED
    processing_started_at: datetime | None = None
    processing_time_expected: int = 0
    task: asyncio.Task[None] | None = None
    error: str | None = None

    @property
    def elapsed(self) -> float:
        """Time since message was received."""
        return (datetime.now() - self.received_at).total_seconds()

    @property
    def processing_elapsed(self) -> float | None:
        """Time since processing started."""
        if self.processing_started_at:
            return (datetime.now() - self.processing_started_at).total_seconds()
        return None

    def status_line(self) -> str:
        """Human-readable status."""
        state_icons = {
            MessageState.QUEUED: "⏳",
            MessageState.PROCESSING: "⚙️ ",
            MessageState.COMMITTING: "💾",
            MessageState.DONE: "✅",
        }
        icon = state_icons[self.state]

        if self.state == MessageState.QUEUED:
            return f"{icon} msg[{self.msg_id}] QUEUED (waiting {self.elapsed:.1f}s)"

        elif self.state == MessageState.PROCESSING:
            progress = (
                (self.processing_elapsed or 0) / self.processing_time_expected * 100
            )
            bar_filled = "█" * int(progress / 10)
            bar_empty = "░" * (10 - int(progress / 10))
            elapsed = self.processing_elapsed
            expected = self.processing_time_expected
            return (
                f"{icon} msg[{self.msg_id}] PROCESSING ({elapsed:.1f}s/{expected}s) "
                f"[{bar_filled}{bar_empty}] {progress:.0f}%"
            )

        elif self.state == MessageState.COMMITTING:
            return f"{icon} msg[{self.msg_id}] COMMITTING offset {self.offset}"

        else:
            return f"{icon} msg[{self.msg_id}] DONE"


@dataclass
class TrackedConsumer:
    """Consumer with detailed in-flight tracking and live display."""

    topic: str
    group_id: str
    max_concurrent: int = 3

    consumer: AIOKafkaConsumer | None = field(default=None, init=False)
    in_flight: dict[int, InFlightMessage] = field(default_factory=dict, init=False)
    completed: list[int] = field(default_factory=list, init=False)
    semaphore: asyncio.Semaphore = field(init=False)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    display_task: asyncio.Task[None] | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

    async def start(self) -> None:
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=self.group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await self.consumer.start()

    async def stop(self) -> None:
        self.stop_event.set()

        if self.display_task:
            self.display_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.display_task

        # Wait for in-flight
        tasks = [m.task for m in self.in_flight.values() if m.task]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if self.consumer:
            await self.consumer.stop()

    async def display_status(self) -> None:
        """Continuously display in-flight message status."""
        while not self.stop_event.is_set():
            # Clear and redraw
            print("\033[2J\033[H")  # Clear screen
            print("=" * 70)
            print("IN-FLIGHT MESSAGE TRACKER")
            print("=" * 70)
            print(f"\nMax concurrent workers: {self.max_concurrent}")
            print(
                f"Completed: {len(self.completed)} | In-flight: {len(self.in_flight)}"
            )
            print("-" * 70)

            if not self.in_flight:
                print("\n  No messages in flight")
            else:
                print("\nIN-FLIGHT MESSAGES:")
                print()
                for msg in sorted(self.in_flight.values(), key=lambda m: m.msg_id):
                    print(f"  {msg.status_line()}")

            print("\n" + "-" * 70)
            print("COMPLETED:", self.completed if self.completed else "None yet")
            print("-" * 70)

            print("\nLEGEND:")
            print("  ⏳ QUEUED     - Received, waiting for available worker")
            print("  ⚙️  PROCESSING - Worker is actively processing")
            print("  💾 COMMITTING - Done processing, committing offset to Kafka")
            print("  ✅ DONE       - Fully complete (removed from in-flight)")

            await asyncio.sleep(0.3)

    async def process_message(self, msg_id: int, processing_time: int) -> bool:
        """Simulate processing work."""
        await asyncio.sleep(processing_time)
        # 10% chance of failure
        if random.random() < 0.1:
            raise ValueError("Simulated error")
        return True

    async def worker(self, msg: Any) -> None:
        """Worker that processes a message."""
        assert self.consumer is not None
        msg_id = msg.value["id"]
        tp = TopicPartition(msg.topic, msg.partition)
        inflight = self.in_flight[msg_id]

        async with self.semaphore:
            # Update state: now processing
            inflight.state = MessageState.PROCESSING
            inflight.processing_started_at = datetime.now()

            try:
                await self.process_message(msg_id, inflight.processing_time_expected)

                # Update state: committing
                inflight.state = MessageState.COMMITTING
                await self.consumer.commit({tp: msg.offset + 1})

                # Done
                inflight.state = MessageState.DONE
                self.completed.append(msg_id)

            except Exception as e:
                inflight.error = str(e)
                # Still commit to avoid reprocessing loop (in prod: send to DLQ)
                await self.consumer.commit({tp: msg.offset + 1})
                self.completed.append(msg_id)

            finally:
                # Small delay to show "done" state
                await asyncio.sleep(0.5)
                if msg_id in self.in_flight:
                    del self.in_flight[msg_id]

    async def run(self, max_messages: int) -> None:
        # Start display task
        assert self.consumer is not None
        self.display_task = asyncio.create_task(self.display_status())

        count = 0
        async for msg in self.consumer:
            if self.stop_event.is_set():
                break

            msg_id = msg.value["id"]
            processing_time = msg.value["processing_time"]

            # Track in-flight message
            inflight = InFlightMessage(
                msg_id=msg_id,
                partition=msg.partition,
                offset=msg.offset,
                received_at=datetime.now(),
                processing_time_expected=processing_time,
                state=MessageState.QUEUED,
            )
            self.in_flight[msg_id] = inflight

            # Spawn worker
            task = asyncio.create_task(self.worker(msg))
            inflight.task = task

            count += 1
            if count >= max_messages:
                break

        # Wait for remaining
        while self.in_flight:
            await asyncio.sleep(0.1)

        # Keep display for a moment to show final state
        await asyncio.sleep(2)


async def produce_messages(count: int = 10) -> None:
    """Send test messages."""
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()

    try:
        for i in range(count):
            processing_time = random.randint(2, 6)
            message = {
                "id": i,
                "processing_time": processing_time,
                "timestamp": datetime.now(UTC).isoformat(),
            }
            await producer.send_and_wait(TOPIC, value=message)
    finally:
        await producer.stop()


async def reset_consumer_group() -> None:
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


async def main() -> None:
    print("Setting up demo...")
    await reset_consumer_group()
    await produce_messages(10)

    print("Starting consumer with live tracking...")
    await asyncio.sleep(1)

    consumer = TrackedConsumer(
        topic=TOPIC,
        group_id=GROUP_ID,
        max_concurrent=3,
    )

    await consumer.start()
    try:
        await consumer.run(max_messages=10)
    finally:
        await consumer.stop()

    # Final summary
    print("\033[2J\033[H")  # Clear screen
    print("=" * 70)
    print("DEMO COMPLETE")
    print("=" * 70)
    print("""
What you saw:

1. QUEUED (⏳)
   - Message received from Kafka
   - Waiting for a worker slot (semaphore)
   - Still "in-flight" - not committed yet

2. PROCESSING (⚙️)
   - Worker acquired semaphore slot
   - Actively doing work (API calls, DB queries, etc.)
   - Progress bar shows estimated completion

3. COMMITTING (💾)
   - Processing finished successfully
   - Committing offset to Kafka
   - Message will not be redelivered after this

4. DONE (✅)
   - Offset committed
   - Removed from in-flight tracking
   - Safe from redelivery

KEY INSIGHT:
   With max_concurrent=3, only 3 messages process simultaneously.
   Others queue up, waiting for a worker slot.
   The consumer loop stays fast -> heartbeats flow -> no timeouts!
""")


if __name__ == "__main__":
    asyncio.run(main())
