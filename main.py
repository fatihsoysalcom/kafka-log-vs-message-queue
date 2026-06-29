import time
import threading

# --- Shared Log (Simulating Kafka's immutable, append-only log) ---
message_log = []
log_lock = threading.Lock() # Protects shared log access
next_message_id = 0

def produce_message(content):
    global next_message_id
    with log_lock:
        message = {"id": next_message_id, "content": content, "timestamp": time.time()}
        message_log.append(message)
        print(f"[PRODUCER] Produced: {message['content']} (ID: {message['id']})")
        next_message_id += 1
    time.sleep(0.05) # Simulate some production delay

# --- Traditional Message Queue Simulation ---
# This queue will have messages removed after consumption
traditional_queue = []
queue_lock = threading.Lock()

class TraditionalQueueConsumer:
    def __init__(self, name):
        self.name = name
        print(f"[{self.name}] Initialized as a Traditional Queue Consumer.")

    def consume(self):
        with queue_lock:
            if traditional_queue:
                message = traditional_queue.pop(0) # Key difference: Messages are removed from the queue
                print(f"[{self.name}] Consumed (and removed from queue): {message['content']} (ID: {message['id']})")
                return message
            else:
                return None

# --- Kafka-like Log Consumer Simulation ---
class KafkaLogConsumer:
    def __init__(self, name):
        self.name = name
        self.offset = 0 # Key difference: Each Kafka-like consumer maintains its own offset
        print(f"[{self.name}] Initialized as a Kafka-like Log Consumer (offset: {self.offset}).")

    def consume(self):
        with log_lock: # Reads from the shared, immutable message_log
            if self.offset < len(message_log):
                message = message_log[self.offset]
                self.offset += 1 # Advances own offset, messages remain in the log
                print(f"[{self.name}] Consumed (from log, offset {self.offset-1}): {message['content']} (ID: {message['id']})")
                return message
            else:
                return None

    def seek_to_offset(self, new_offset):
        # Key difference: Kafka consumers can "rewind" or "fast-forward" their offset
        self.offset = new_offset
        print(f"[{self.name}] Seeked to offset: {self.offset}")

# --- Main Simulation Logic ---
def run_simulation():
    print("--- SIMULATION START: Kafka vs. Traditional Queue ---")

    print("\n--- Phase 1: Traditional Message Queue Behavior ---")
    # Producer adds messages to both the Kafka-like log and the traditional queue
    for i in range(3):
        msg_content = f"Queue Message {i+1}"
        produce_message(msg_content) # Also add to Kafka-like log for later comparison
        with queue_lock:
            traditional_queue.append({"id": next_message_id - 1, "content": msg_content, "timestamp": time.time()})

    print("\n--- Traditional Queue Consumer 1 consumes ---")
    queue_consumer_1 = TraditionalQueueConsumer("Q-Consumer-1")
    for _ in range(3):
        queue_consumer_1.consume()
    print(f"[Traditional Queue] Current state after Q-Consumer-1: {len(traditional_queue)} messages left.")

    print("\n--- Traditional Queue Consumer 2 tries to consume ---")
    queue_consumer_2 = TraditionalQueueConsumer("Q-Consumer-2")
    for _ in range(3):
        if queue_consumer_2.consume() is None:
            print(f"[Q-Consumer-2] No messages left. (Demonstrates messages are removed after consumption)")
            break
    print(f"[Traditional Queue] Current state after Q-Consumer-2: {len(traditional_queue)} messages left.")


    print("\n--- Phase 2: Kafka-like Log Behavior ---")
    # Producer adds more messages to the Kafka-like log
    for i in range(3):
        produce_message(f"Kafka Log Message {i+1}")

    print("\n--- Kafka Log Consumer 1 consumes ---")
    kafka_consumer_1 = KafkaLogConsumer("K-Consumer-1")
    # K-Consumer-1 starts from offset 0, reading all messages produced so far
    while True:
        msg = kafka_consumer_1.consume()
        if msg is None:
            print(f"[K-Consumer-1] No new messages for now.")
            break
        time.sleep(0.05)

    print("\n--- Kafka Log Consumer 2 consumes (independently) ---")
    kafka_consumer_2 = KafkaLogConsumer("K-Consumer-2")
    # K-Consumer-2 also starts from offset 0 and reads all messages, demonstrating independent consumption
    while True:
        msg = kafka_consumer_2.consume()
        if msg is None:
            print(f"[K-Consumer-2] No new messages for now.")
            break
        time.sleep(0.05)

    print("\n--- Producer adds even more messages ---")
    for i in range(2):
        produce_message(f"New Kafka Log Message {i+1}")

    print("\n--- K-Consumer-1 continues consuming new messages ---")
    # K-Consumer-1 picks up from where it left off
    while True:
        msg = kafka_consumer_1.consume()
        if msg is None:
            print(f"[K-Consumer-1] No new messages for now.")
            break
        time.sleep(0.05)

    print("\n--- K-Consumer-2 tries to re-read (seeks to an earlier offset) ---")
    kafka_consumer_2.seek_to_offset(0) # Key difference: Rewind K-Consumer-2 to the beginning of the log
    while True:
        msg = kafka_consumer_2.consume()
        if msg is None:
            print(f"[K-Consumer-2] Finished re-reading from offset 0.")
            break
        time.sleep(0.05)

    print("\n--- SIMULATION END ---")
    print(f"Final state of Kafka-like log: {len(message_log)} messages. (Messages are retained)")
    print(f"Final state of Traditional Queue: {len(traditional_queue)} messages. (Messages were removed)")
    print("Notice how Kafka-like log messages are never removed, allowing multiple consumers and re-reads.")

if __name__ == "__main__":
    run_simulation()
