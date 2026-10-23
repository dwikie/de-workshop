
import json
import os
from datetime import datetime
from decimal import Decimal

from pyflink.common import Configuration, Row, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.formats.parquet import ParquetBulkWriters
from pyflink.table import DataTypes
from pyflink.common.serialization import SimpleStringSchema

# Example Kafka message:
# {
#         "transaction_id": "txn-000005",
#         "account_id": "acct-0048",
#         "amount": 2400.00,
#         "currency": "USD",
#         "event_time": "2026-10-23T10:15:32Z",
#         "country": "ID",
#         "status": "APPROVED"
# }

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka.data-platform.svc.cluster.local:9092",
)
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "transactions-parquet-v1")

OUTPUT_PATH = os.getenv(
    "OUTPUT_PATH",
    "s3a://sink/transactions/",
)

# Flink execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_parallelism(1)

env.enable_checkpointing(30_0000)

# Kafka source
source = (
    KafkaSource.builder()
    .set_bootstrap_servers(BOOTSTRAP_SERVERS)
    .set_topics(TOPIC)
    .set_group_id(GROUP_ID)
    .set_starting_offsets(KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

raw_stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "transactions-source",
)

# Typed schema for Parquet.
row_type = DataTypes.ROW([
    DataTypes.FIELD("transaction_id", DataTypes.STRING()),
    DataTypes.FIELD("account_id", DataTypes.STRING()),
    DataTypes.FIELD("amount", DataTypes.DECIMAL(18, 2)),
    DataTypes.FIELD("currency", DataTypes.STRING()),
    DataTypes.FIELD("event_time", DataTypes.STRING()),
    DataTypes.FIELD("country", DataTypes.STRING()),
    DataTypes.FIELD("status", DataTypes.STRING()),
])

row_type_info = Types.ROW_NAMED(
    [
        "transaction_id",
        "account_id",
        "amount",
        "currency",
        "event_time",
        "country",
        "status",
    ],
    [
        Types.STRING(),
        Types.STRING(),
        Types.BIG_DEC(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
    ],
)


def parse_transaction(message: str) -> Row:
    obj = json.loads(message)

    return Row(
        obj["transaction_id"],
        obj["account_id"],
        Decimal(str(obj["amount"])),
        obj["currency"],
        obj["event_time"],
        obj["country"],
        obj["status"],
    )


transactions = raw_stream.map(
    parse_transaction,
    output_type=row_type_info,
)

output_config = (
    OutputFileConfig.builder()
    .with_part_prefix("part")
    .with_part_suffix(".parquet")
    .build()
)

sink = (
    FileSink.for_bulk_format(
        OUTPUT_PATH,
        ParquetBulkWriters.for_row_type(
            row_type,
            hadoop_config=Configuration(),
            utc_timestamp=True,
        ),
    )
    .with_output_file_config(output_config)
    .build()
)

transactions.sink_to(sink)

env.execute("kafka-transactions-to-parquet")
