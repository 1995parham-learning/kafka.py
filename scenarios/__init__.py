"""
Kafka Demo Scenarios

Available scenarios:
- offset_ack_behavior: Shows what happens when you ack message N+1 without acking N
- dead_letter_queue: Proper solution using DLQ pattern
- slow_processing: What happens when message processing takes too long
- async_worker_pool: Async concurrent processing (the solution)
- inflight_tracking: Live visualization of in-flight message states
"""
