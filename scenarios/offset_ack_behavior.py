"""
Scenario: Acknowledging message N+1 without acknowledging message N

This demonstrates Kafka's offset-based commit model:
- Kafka doesn't have per-message acks like RabbitMQ
- Committing offset N means "I've processed all messages up to N"
- If you commit offset 6 without committing offset 5, offset 5 is also considered committed

This scenario shows:
1. Producer sends 5 messages (offsets 0-4)
2. Consumer receives all messages with manual commit disabled
3. Consumer "fails" to process message at offset 2 (simulated failure)
4. Consumer commits offset 4 (skipping offset 2)
5. On restart, consumer will NOT reprocess offset 2 - it's lost!

Run this to see the behavior:
    uv run python scenarios/offset_ack_behavior.py
"""

import asyncio
import json
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition


TOPIC = "offset-ack-demo"
BOOTSTRAP_SERVERS = "localhost:29092"
GROUP_ID = "offset-ack-group"


async def produce_messages(count: int = 5):
    """Send test messages."""
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()

    try:
        print(f"\n{'='*60}")
        print("PRODUCING MESSAGES")
        print("=" * 60)

        for i in range(count):
            message = {
                "id": i,
                "data": f"Message {i}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            result = await producer.send_and_wait(TOPIC, value=message)
            print(f"Sent message {i} -> partition={result.partition}, offset={result.offset}")

    finally:
        await producer.stop()


async def consume_with_selective_ack():
    """
    Consume messages and demonstrate selective acknowledgment.

    We'll "fail" to process message 2, but commit message 4.
    This shows that message 2 will be lost because Kafka commits are offset-based.
    """
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # Manual commit control
    )
    await consumer.start()

    try:
        print(f"\n{'='*60}")
        print("CONSUMING WITH SELECTIVE ACK (First Run)")
        print("=" * 60)
        print("Strategy: Process all messages, but 'fail' on message 2")
        print("          Then commit offset 4, skipping offset 2\n")

        failed_offsets: list[int] = []
        processed_offsets: list[int] = []

        # Consume messages with a timeout
        async def consume_batch():
            async for msg in consumer:
                offset = msg.offset
                value = msg.value
                tp = TopicPartition(msg.topic, msg.partition)

                # Simulate failure on message 2
                if value["id"] == 2:
                    print(f"  OFFSET {offset}: FAILED to process (simulated) - {value['data']}")
                    failed_offsets.append(offset)
                else:
                    print(f"  OFFSET {offset}: Processed successfully - {value['data']}")
                    processed_offsets.append(offset)

                # After processing message 4, commit and break
                if value["id"] == 4:
                    # Commit offset 5 (next offset to read)
                    # This implicitly "acks" ALL messages 0-4, including the failed one!
                    print(f"\n  Committing offset {offset + 1} (next offset after {offset})")
                    await consumer.commit({tp: offset + 1})
                    break

        # Use wait_for to add timeout
        try:
            await asyncio.wait_for(consume_batch(), timeout=10.0)
        except asyncio.TimeoutError:
            print("  No messages received (timeout)")
            return

        print(f"\n  Processed offsets: {processed_offsets}")
        print(f"  Failed offsets: {failed_offsets}")
        print(f"\n  WARNING: Offset 2 was 'failed' but is now committed!")

    finally:
        await consumer.stop()


async def consume_after_restart():
    """
    Simulate consumer restart to show that message 2 is lost.
    """
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()

    try:
        print(f"\n{'='*60}")
        print("CONSUMING AFTER 'RESTART' (Second Run)")
        print("=" * 60)
        print("Expecting: No messages (all were committed, including failed one)\n")

        async def consume_batch():
            async for msg in consumer:
                print(f"  OFFSET {msg.offset}: {msg.value['data']}")

        try:
            await asyncio.wait_for(consume_batch(), timeout=5.0)
        except asyncio.TimeoutError:
            print("  No messages to consume - offset 2 was lost!")
            print("  The failed message will never be reprocessed.")

    finally:
        await consumer.stop()


async def reset_consumer_group():
    """Reset consumer group to start fresh."""
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()

    try:
        # Seek to beginning
        await consumer.seek_to_beginning()
        # Commit offset 0 to reset
        for tp in consumer.assignment():
            await consumer.commit({tp: 0})
        print("Consumer group reset to beginning")
    finally:
        await consumer.stop()


async def main():
    print("\n" + "=" * 60)
    print("KAFKA OFFSET ACK BEHAVIOR DEMO")
    print("=" * 60)
    print("""
This demo shows a critical Kafka behavior:

  Kafka uses OFFSET-BASED commits, not per-message acks.

  When you commit offset N, you're saying:
  "I have processed ALL messages from 0 to N-1"

  If you fail to process message 2 but commit message 4,
  message 2 is LOST - it won't be redelivered!

This is different from RabbitMQ where you can nack individual messages.
""")

    # Reset consumer group for clean demo
    await reset_consumer_group()

    # Produce test messages
    await produce_messages(5)

    # First consumption - with selective "failure"
    await consume_with_selective_ack()

    # Simulate restart - message 2 should be lost
    await consume_after_restart()

    print(f"\n{'='*60}")
    print("SOLUTION: Use a Dead Letter Queue or local tracking")
    print("=" * 60)
    print("""
To handle failed messages properly in Kafka:

1. Dead Letter Queue (DLQ):
   - Send failed messages to a separate "dead-letter" topic
   - Process DLQ separately with retry logic

2. Local State Tracking:
   - Track failed offsets in a database/file
   - Reprocess from tracked state on restart

3. Transactional Processing:
   - Use Kafka transactions for exactly-once semantics
   - Requires careful setup and impacts performance

4. Don't commit until all messages in batch succeed:
   - Only commit after successfully processing all messages
   - Risk: reprocessing already-processed messages on failure
""")


if __name__ == "__main__":
    asyncio.run(main())
