"""
Scenario: Slow Message Processing

This demonstrates what happens when processing a message takes too long:
- Consumer may miss heartbeats and get kicked from the group
- Partition rebalancing occurs
- The slow message gets reprocessed (duplicate processing!)

Key settings:
- session_timeout_ms: Max time between heartbeats before consumer is considered dead
- max_poll_interval_ms: Max time between poll() calls
- heartbeat_interval_ms: How often to send heartbeats

Run this to see the behavior:
    uv run python scenarios/slow_processing.py
"""

import asyncio
import contextlib
import json
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

TOPIC = "slow-processing-demo"
BOOTSTRAP_SERVERS = "localhost:29092"
GROUP_ID = "slow-processor-group"


async def produce_messages(count: int = 5):
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
            # Message 2 will require "slow" processing
            processing_time = 15 if i == 2 else 1
            message = {
                "id": i,
                "data": f"Message {i}",
                "processing_time_seconds": processing_time,
                "timestamp": datetime.now(UTC).isoformat(),
            }
            await producer.send_and_wait(TOPIC, value=message)
            marker = " <- SLOW (15s)" if i == 2 else ""
            print(f"  Message {i}: processing_time={processing_time}s{marker}")

    finally:
        await producer.stop()


async def consume_with_default_timeout():
    """
    Consume with default timeouts - will fail on slow message.

    Default max_poll_interval_ms is 300000 (5 min), but we'll set it lower
    to demonstrate the issue faster.
    """
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        # Short timeout to demonstrate the problem quickly
        max_poll_interval_ms=10000,  # 10 seconds max between polls
        session_timeout_ms=10000,  # 10 seconds session timeout
    )
    await consumer.start()

    try:
        print(f"\n{'=' * 60}")
        print("CONSUMING WITH SHORT TIMEOUT (Will Fail)")
        print("=" * 60)
        print("Settings: max_poll_interval_ms=10000, session_timeout_ms=10000")
        print("Message 2 takes 15s to process -> WILL EXCEED TIMEOUT\n")

        processed = []

        async def process_messages():
            async for msg in consumer:
                value = msg.value
                tp = TopicPartition(msg.topic, msg.partition)
                processing_time = value["processing_time_seconds"]

                print(
                    f"  Processing message {value['id']} (takes {processing_time}s)..."
                )
                start = datetime.now()

                # Simulate slow processing
                await asyncio.sleep(processing_time)

                elapsed = (datetime.now() - start).total_seconds()
                print(f"  ✓ Message {value['id']} done in {elapsed:.1f}s")

                await consumer.commit({tp: msg.offset + 1})
                processed.append(value["id"])

        try:
            await asyncio.wait_for(process_messages(), timeout=60.0)
        except TimeoutError:
            pass
        except Exception as e:
            print(f"\n  ERROR: {type(e).__name__}: {e}")
            print("  The consumer was kicked from the group due to timeout!")

        print(f"\n  Processed messages: {processed}")

    finally:
        await consumer.stop()


async def consume_with_proper_config():
    """
    Consume with properly configured timeouts for slow processing.
    """
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"{GROUP_ID}-proper",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        # Increased timeouts for slow processing
        max_poll_interval_ms=60000,  # 60 seconds max between polls
        session_timeout_ms=30000,  # 30 seconds session timeout
        heartbeat_interval_ms=3000,  # 3 seconds heartbeat
    )
    await consumer.start()

    try:
        print(f"\n{'=' * 60}")
        print("CONSUMING WITH PROPER TIMEOUT CONFIG")
        print("=" * 60)
        print("Settings: max_poll_interval_ms=60000, session_timeout_ms=30000")
        print("Message 2 takes 15s -> Within timeout, will succeed\n")

        processed = []

        async def process_messages():
            async for msg in consumer:
                value = msg.value
                tp = TopicPartition(msg.topic, msg.partition)
                processing_time = value["processing_time_seconds"]

                print(
                    f"  Processing message {value['id']} (takes {processing_time}s)..."
                )
                start = datetime.now()

                await asyncio.sleep(processing_time)

                elapsed = (datetime.now() - start).total_seconds()
                print(f"  ✓ Message {value['id']} done in {elapsed:.1f}s")

                await consumer.commit({tp: msg.offset + 1})
                processed.append(value["id"])

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(process_messages(), timeout=60.0)

        print(f"\n  Processed messages: {processed}")

    finally:
        await consumer.stop()


async def consume_with_background_processing():
    """
    Best practice: Process messages in background while keeping consumer responsive.

    This pattern:
    1. Polls messages quickly (keeps heartbeats flowing)
    2. Processes in background tasks
    3. Tracks pending work
    4. Commits only after processing completes
    """
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"{GROUP_ID}-background",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_interval_ms=30000,
    )
    await consumer.start()

    try:
        print(f"\n{'=' * 60}")
        print("CONSUMING WITH BACKGROUND PROCESSING (Best Practice)")
        print("=" * 60)
        print("Pattern: Poll quickly, process in background tasks")
        print("Benefits: Consumer stays responsive, no timeout issues\n")

        pending_tasks: dict[int, asyncio.Task] = {}
        processed: list[int] = []

        async def process_in_background(
            msg_id: int, processing_time: int, tp: TopicPartition, offset: int
        ):
            """Background task for processing a single message."""
            print(f"  [BG] Starting message {msg_id} (takes {processing_time}s)")
            await asyncio.sleep(processing_time)
            print(f"  [BG] ✓ Message {msg_id} complete")
            return (msg_id, tp, offset)

        async def process_messages():
            async for msg in consumer:
                value = msg.value
                msg_id = value["id"]
                tp = TopicPartition(msg.topic, msg.partition)
                processing_time = value["processing_time_seconds"]

                print(f"  Received message {msg_id}, spawning background task...")

                # Start background processing
                task = asyncio.create_task(
                    process_in_background(msg_id, processing_time, tp, msg.offset)
                )
                pending_tasks[msg_id] = task

                # Check for completed tasks and commit
                done_ids = []
                for task_id, task in pending_tasks.items():
                    if task.done():
                        result_id, result_tp, result_offset = task.result()
                        await consumer.commit({result_tp: result_offset + 1})
                        processed.append(result_id)
                        done_ids.append(task_id)
                        print(f"  [COMMIT] Message {result_id} committed")

                for done_id in done_ids:
                    del pending_tasks[done_id]

            # Wait for remaining tasks
            if pending_tasks:
                print(f"\n  Waiting for {len(pending_tasks)} pending tasks...")
                results = await asyncio.gather(*pending_tasks.values())
                for result_id, result_tp, result_offset in results:
                    await consumer.commit({result_tp: result_offset + 1})
                    processed.append(result_id)
                    print(f"  [COMMIT] Message {result_id} committed")

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(process_messages(), timeout=60.0)

        print(f"\n  Processed messages: {sorted(processed)}")

    finally:
        await consumer.stop()


async def reset_consumer_groups():
    """Reset all consumer groups for clean demo."""
    groups = [GROUP_ID, f"{GROUP_ID}-proper", f"{GROUP_ID}-background"]
    for group in groups:
        try:
            consumer = AIOKafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=group,
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
    print("SLOW MESSAGE PROCESSING DEMO")
    print("=" * 60)
    print(
        """
This demo shows what happens when message processing is slow:

Problem:
  - Kafka consumers must send heartbeats to stay in the group
  - If processing takes too long, consumer misses heartbeats
  - Consumer gets kicked, partition rebalances, message reprocessed!

Key settings:
  - max_poll_interval_ms: Max time between poll() calls (default: 5 min)
  - session_timeout_ms: Max time between heartbeats (default: 45s)
  - heartbeat_interval_ms: How often to heartbeat (default: 3s)

Solutions:
  1. Increase timeouts (simple but not always practical)
  2. Process in background tasks (best for async Python)
  3. Use pause/resume to stop fetching during heavy processing
"""
    )

    await reset_consumer_groups()
    await produce_messages(5)

    # Demo 1: Short timeout (will fail)
    await consume_with_default_timeout()

    # Reset for next demo
    await reset_consumer_groups()
    await produce_messages(5)

    # Demo 2: Proper timeout config
    await consume_with_proper_config()

    # Reset for next demo
    await reset_consumer_groups()
    await produce_messages(5)

    # Demo 3: Background processing (best practice)
    await consume_with_background_processing()

    print(f"\n{'=' * 60}")
    print("KEY TAKEAWAYS")
    print("=" * 60)
    print(
        """
1. Know your processing time and set timeouts accordingly
2. For unpredictable processing times, use background tasks
3. aiokafka handles heartbeats automatically in the background
4. Consider using consumer.pause() for very long operations
5. Monitor consumer lag to detect slow processing issues
"""
    )


if __name__ == "__main__":
    asyncio.run(main())
