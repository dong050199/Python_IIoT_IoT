from pykafka import KafkaClient
from pykafka.common import OffsetType

client = KafkaClient(hosts='localhost:9092')
topic = client.topics['TestTopic']
consumer1 = topic.get_simple_consumer(
    auto_offset_reset=OffsetType.LATEST,
    reset_offset_on_start=True)


for message in consumer1:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`

    print(message.value)