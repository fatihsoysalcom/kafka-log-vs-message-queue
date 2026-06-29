# Kafka Log vs Message Queue

This example demonstrates the fundamental difference between a traditional message queue and a Kafka-like append-only log. It shows how traditional queues remove messages after consumption, preventing other consumers from seeing them, while a Kafka-like log retains messages, allowing multiple independent consumers to read the same data from their own offsets, and even re-read past messages. This highlights why Kafka is not merely a message queue.

## Language

`python`

## How to Run

Save the code as `main.py` and run from your terminal:
`python main.py`

## Original Article

This example accompanies the Turkish article: [Kafka Bir Kuyruk Değildir: Sisteminizi Mahvetmekten Nasıl Kaçınırsınız?](https://fatihsoysal.com/blog/kafka-bir-kuyruk-degildir-sisteminizi-mahvetmekten-nasil-kacinirsiniz/).

## License

MIT — see [LICENSE](LICENSE).
