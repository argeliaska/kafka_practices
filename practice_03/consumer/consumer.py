from kafka import KafkaConsumer
import json


class Consumer:
    def __init__(self, topic):
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=["localhost:9092"],
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            group_id="nflx",
        )
        self.data = []

    @property
    def consumer(self):
        return self._consumer

    @consumer.setter
    def consumer(self, value):
        if isinstance(value, KafkaConsumer):
            self._consumer = value

    def start_read(self):
        self.receive_message()

    def receive_message(self):
        message_count = 0
        for message in self._consumer:
            message = message.value
            print(f"Message {message_count}: {message}")
            self.data.append(message)
            message_count += 1


if __name__ == "__main__":
    pass
