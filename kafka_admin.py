from ensurepip import bootstrap
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
consumer = KafkaConsumer("position_history", bootstrap_servers='127.0.0.1:9092', auto_offset_reset="latest", enable_auto_commit=True, group_id="consumers1")
admin = KafkaAdminClient(bootstrap_servers='127.0.0.1:9092')

producer.bootstrap_connected()
consumer.bootstrap_connected()