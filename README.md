# kafka-log-vs-message-queue
This example demonstrates the fundamental difference between a traditional message queue and a Kafka-like append-only log. It shows how traditional queues remove messages after consumption, preventing other consumers from seeing them, while a Kafka-like log retains messages, allowing multiple independent consumers to read the same data from their o
