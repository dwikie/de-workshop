
import os

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
)
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka.data-platform.svc.cluster.local:9092",
)
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "pyflink-smoke-test")

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_parallelism(1)

source = (
    KafkaSource.builder()
    .set_bootstrap_servers(BOOTSTRAP_SERVERS)
    .set_topics(TOPIC)
    .set_group_id(GROUP_ID)
    .set_starting_offsets(KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

events = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "transactions-kafka-source",
)

events.print()

env.execute("kafka-smoke-test")
