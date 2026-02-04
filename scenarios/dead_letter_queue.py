"""
Scenario: Dead Letter Queue (DLQ) Pattern

This demonstrates the proper way to handle failed messages in Kafka:
- Messages that fail processing are sent to a Dead Letter Queue
- The main consumer can continue and commit offsets
- A separate DLQ consumer handles retries/alerts

Run this to see the behavior:
    uv run python scenarios/dead_letter_queue.py
"""

import asyncio
import contextlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

MAIN_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
BOOTSTRAP_SERVERS = "localhost:29092"
GROUP_ID = "orders-processor"


@dataclass
class ProcessingResult:
    success: bool
    error: str | None = None


async def process_order(order: dict) -> ProcessingResult:
    """
    Simulate order processing.
    Orders with id % 3 == 2 will "fail" (simulating random failures).
    """
    await asyncio.sleep(0.1)  # Simulate processing time

    if order["id"] % 3 == 2:
        return ProcessingResult(
            success=False, error=f"Payment gateway timeout for order {order['id']}"
        )

    return ProcessingResult(success=True)


async def produce_orders(count: int = 10):
    """Send test orders."""
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()

    try:
        print(f"\n{'=' * 60}")
        print("PRODUCING ORDERS")
        print("=" * 60)

        for i in range(count):
            order = {
                "id": i,
                "product": f"Product-{i}",
                "amount": 100 + i * 10,
                "timestamp": datetime.now(UTC).isoformat(),
            }
            result = await producer.send_and_wait(MAIN_TOPIC, value=order)
            print(f"  Order {i}: ${order['amount']} -> offset={result.offset}")

    finally:
        await producer.stop()


async def consume_with_dlq():
    """
    Consume orders with Dead Letter Queue pattern.
    Failed messages go to DLQ, allowing main processing to continue.
    """
    consumer = AIOKafkaConsumer(
        MAIN_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    # Producer for DLQ
    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await consumer.start()
    await dlq_producer.start()

    try:
        print(f"\n{'=' * 60}")
        print("CONSUMING WITH DLQ PATTERN")
        print("=" * 60)
        print(f"Main topic: {MAIN_TOPIC}")
        print(f"DLQ topic:  {DLQ_TOPIC}\n")

        processed = 0
        failed = 0

        async def process_batch():
            nonlocal processed, failed

            async for msg in consumer:
                order = msg.value
                tp = TopicPartition(msg.topic, msg.partition)

                result = await process_order(order)

                if result.success:
                    print(f"  ✓ Order {order['id']}: Processed successfully")
                    processed += 1
                else:
                    print(f"  ✗ Order {order['id']}: FAILED - {result.error}")
                    print("    -> Sending to DLQ")

                    # Send to Dead Letter Queue with metadata
                    dlq_message = {
                        "original_message": order,
                        "error": result.error,
                        "original_topic": msg.topic,
                        "original_partition": msg.partition,
                        "original_offset": msg.offset,
                        "failed_at": datetime.now(UTC).isoformat(),
                        "retry_count": 0,
                    }
                    await dlq_producer.send_and_wait(DLQ_TOPIC, value=dlq_message)
                    failed += 1

                # Commit after each message (or batch in production)
                await consumer.commit({tp: msg.offset + 1})

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(process_batch(), timeout=10.0)

        print(f"\n  Summary: {processed} processed, {failed} sent to DLQ")

    finally:
        await consumer.stop()
        await dlq_producer.stop()


async def consume_dlq():
    """
    Consume from Dead Letter Queue.
    In production, this would implement retry logic or alerting.
    """
    consumer = AIOKafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"{GROUP_ID}-dlq",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    await consumer.start()

    try:
        print(f"\n{'=' * 60}")
        print("DLQ CONSUMER (Retry Handler)")
        print("=" * 60)
        print("Processing failed messages from DLQ:\n")

        async def process_dlq():
            async for msg in consumer:
                dlq_msg = msg.value
                original = dlq_msg["original_message"]

                print("  DLQ Message:")
                print(f"    Original Order: {original['id']}")
                print(f"    Error: {dlq_msg['error']}")
                print(f"    Failed at: {dlq_msg['failed_at']}")
                print(f"    Retry count: {dlq_msg['retry_count']}")
                print()

                # In production, you would:
                # 1. Retry processing
                # 2. Send to another DLQ if retry fails
                # 3. Alert operators after N retries
                # 4. Store in database for manual review

        try:
            await asyncio.wait_for(process_dlq(), timeout=5.0)
        except TimeoutError:
            print("  No more messages in DLQ")

    finally:
        await consumer.stop()


async def reset_consumer_groups():
    """Reset consumer groups for clean demo."""
    for group in [GROUP_ID, f"{GROUP_ID}-dlq"]:
        for topic in [MAIN_TOPIC, DLQ_TOPIC]:
            try:
                consumer = AIOKafkaConsumer(
                    topic,
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
    print("DEAD LETTER QUEUE (DLQ) PATTERN DEMO")
    print("=" * 60)
    print("""
The DLQ pattern solves the "lost message" problem:

1. Main consumer processes messages
2. Failed messages are sent to a DLQ topic
3. Main consumer commits and continues
4. Separate DLQ consumer handles retries/alerts

Benefits:
- No lost messages
- Main processing continues despite failures
- Failed messages can be retried or reviewed
- Clear audit trail of failures
""")

    await reset_consumer_groups()
    await produce_orders(10)
    await consume_with_dlq()
    await consume_dlq()

    print(f"\n{'=' * 60}")
    print("KEY TAKEAWAYS")
    print("=" * 60)
    print("""
1. Always use DLQ for production Kafka consumers
2. Include metadata: original offset, error, timestamp, retry count
3. Implement retry limits to prevent infinite loops
4. Monitor DLQ size for alerting
5. Consider using Kafka transactions for exactly-once semantics
""")


if __name__ == "__main__":
    asyncio.run(main())
